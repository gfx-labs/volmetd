package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gfx-labs/volmetd/pkg/collector"
	"github.com/gfx-labs/volmetd/pkg/config"
	"github.com/gfx-labs/volmetd/pkg/discovery"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("volmetd starting...")

	cfg := config.FromEnv()
	log.Printf("config: listen=%s metrics=%s", cfg.ListenAddr, cfg.MetricsPath)
	log.Printf("config: hostProc=%s kubelet=%s", cfg.HostProcPath, cfg.KubeletPath)
	log.Printf("config: discovery=%v", cfg.DiscoveryMethods)
	if len(cfg.Namespaces) > 0 {
		log.Printf("config: namespaces=%v", cfg.Namespaces)
	} else {
		log.Println("config: namespaces=all")
	}

	// Build discoverers in configured order
	var discoverers []discovery.Discoverer

	for _, method := range cfg.DiscoveryMethods {
		switch method {
		case config.DiscoveryCSI:
			csi := discovery.NewCSIDiscoverer(cfg.KubeletPath, cfg.MountsPath())
			discoverers = append(discoverers, csi)
			log.Printf("enabled discoverer: %s", method)

		case config.DiscoveryK8sAPI:
			k8s, err := discovery.NewK8sAPIDiscoverer(cfg.KubeletPath, cfg.MountsPath(), cfg.Namespaces)
			if err != nil {
				log.Printf("warning: discoverer %s disabled: %v", method, err)
			} else {
				discoverers = append(discoverers, k8s)
				log.Printf("enabled discoverer: %s", method)
			}

		default:
			log.Printf("warning: unknown discovery method: %s", method)
		}
	}

	if len(discoverers) == 0 {
		log.Fatal("no discoverers available")
	}

	multi := discovery.NewMultiDiscoverer(discoverers...)

	// Create collectors
	diskstats := collector.NewDiskstatsCollector(cfg.HostProcPath)
	capacity := collector.NewCapacityCollector()

	// Create and register volume collector
	vc := collector.NewVolumeCollector(multi, diskstats, capacity)
	prometheus.MustRegister(vc)

	// HTTP server
	mux := http.NewServeMux()
	mux.Handle(cfg.MetricsPath, promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Graceful shutdown
	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
		close(done)
	}()

	log.Printf("listening on %s", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("listen error: %v", err)
	}

	<-done
	log.Println("goodbye")
}
