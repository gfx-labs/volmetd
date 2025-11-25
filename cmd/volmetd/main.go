package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gfx-labs/volmetd/pkg/collector"
	"github.com/gfx-labs/volmetd/pkg/config"
	"github.com/gfx-labs/volmetd/pkg/discovery"
)

func main() {
	// Setup slog with debug level if VOLMETD_DEBUG is set
	level := slog.LevelInfo
	if v := strings.ToLower(os.Getenv("VOLMETD_DEBUG")); v == "1" || v == "true" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	slog.Info("volmetd starting")

	cfg := config.FromEnv()
	slog.Info("config", "listen", cfg.ListenAddr, "metrics", cfg.MetricsPath)
	slog.Info("config", "hostProc", cfg.HostProcPath, "kubelet", cfg.KubeletPath)
	slog.Info("config", "discovery", cfg.DiscoveryMethods)
	if len(cfg.Namespaces) > 0 {
		slog.Info("config", "namespaces", cfg.Namespaces)
	} else {
		slog.Info("config", "namespaces", "all")
	}

	// Build discoverers in configured order
	var discoverers []discovery.Discoverer

	for _, method := range cfg.DiscoveryMethods {
		switch method {
		case config.DiscoveryCSI:
			csi := discovery.NewCSIDiscoverer(cfg.KubeletPath, cfg.MountsPath())
			discoverers = append(discoverers, csi)
			slog.Info("enabled discoverer", "method", method)

		case config.DiscoveryK8sAPI:
			k8s, err := discovery.NewK8sAPIDiscoverer(cfg.KubeletPath, cfg.MountsPath(), cfg.Namespaces)
			if err != nil {
				slog.Warn("discoverer disabled", "method", method, "error", err)
			} else {
				discoverers = append(discoverers, k8s)
				slog.Info("enabled discoverer", "method", method)
			}

		default:
			slog.Warn("unknown discovery method", "method", method)
		}
	}

	if len(discoverers) == 0 {
		slog.Error("no discoverers available")
		os.Exit(1)
	}

	multi := discovery.NewMultiDiscoverer(discoverers...)

	// Create collectors
	diskstats := collector.NewDiskstatsCollector(cfg.HostProcPath)
	capacity := collector.NewCapacityCollector()

	// Create and register volume collector
	vc := collector.NewVolumeCollector(multi, cfg.HostProcPath, diskstats, capacity)
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
		slog.Info("shutting down")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			slog.Error("shutdown error", "error", err)
		}
		close(done)
	}()

	slog.Info("listening", "addr", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("listen error", "error", err)
		os.Exit(1)
	}

	<-done
	slog.Info("goodbye")
}
