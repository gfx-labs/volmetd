package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gfx-labs/volmetd/pkg/discovery"
	"github.com/gfx-labs/volmetd/pkg/diskstats"
)

// Collector collects metrics for discovered volumes
type Collector interface {
	// Name returns the collector name
	Name() string
	// Update collects metrics and sends them to the channel
	Update(volumes []*discovery.VolumeInfo, ch chan<- prometheus.Metric) error
}

var (
	scrapeDurationDesc = prometheus.NewDesc(
		"volmetd_scrape_duration_seconds",
		"Time spent collecting metrics by collector",
		[]string{"collector"}, nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		"volmetd_scrape_success",
		"Whether the collector succeeded",
		[]string{"collector"}, nil,
	)
	volumesDiscoveredDesc = prometheus.NewDesc(
		"volmetd_volumes_discovered",
		"Number of PVC volumes discovered",
		nil, nil,
	)
)

// VolumeCollector orchestrates all sub-collectors
type VolumeCollector struct {
	discoverer *discovery.MultiDiscoverer
	collectors []Collector
	procPath   string
}

// NewVolumeCollector creates a new volume collector
func NewVolumeCollector(discoverer *discovery.MultiDiscoverer, procPath string, collectors ...Collector) *VolumeCollector {
	if procPath == "" {
		procPath = "/proc"
	}
	return &VolumeCollector{
		discoverer: discoverer,
		collectors: collectors,
		procPath:   procPath,
	}
}

// Describe implements prometheus.Collector
func (v *VolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
	ch <- volumesDiscoveredDesc
}

// Collect implements prometheus.Collector
func (v *VolumeCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.Background()

	// Discover volumes
	start := time.Now()
	volumes, err := v.discoverer.Discover(ctx)
	duration := time.Since(start).Seconds()

	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration, "discovery")
	if err != nil {
		slog.Error("discovery error", "error", err)
		ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, 0, "discovery")
		return
	}
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, 1, "discovery")
	ch <- prometheus.MustNewConstMetric(volumesDiscoveredDesc, prometheus.GaugeValue, float64(len(volumes)))

	// Resolve device names from diskstats before running collectors
	v.resolveDeviceNames(volumes)

	// Run collectors in parallel
	wg := sync.WaitGroup{}
	wg.Add(len(v.collectors))

	for _, c := range v.collectors {
		go func(c Collector) {
			defer wg.Done()
			v.execute(c, volumes, ch)
		}(c)
	}

	wg.Wait()
}

func (v *VolumeCollector) execute(c Collector, volumes []*discovery.VolumeInfo, ch chan<- prometheus.Metric) {
	start := time.Now()
	err := c.Update(volumes, ch)
	duration := time.Since(start).Seconds()

	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration, c.Name())

	if err != nil {
		slog.Error("collector error", "collector", c.Name(), "error", err)
		ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, 0, c.Name())
		return
	}
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, 1, c.Name())
}

// resolveDeviceNames resolves device names from diskstats using device IDs
func (v *VolumeCollector) resolveDeviceNames(volumes []*discovery.VolumeInfo) {
	stats, err := diskstats.Parse(v.procPath + "/diskstats")
	if err != nil {
		slog.Error("failed to parse diskstats", "error", err)
		return
	}

	slog.Debug("diskstats parsed", "byName", len(stats.ByName), "byID", len(stats.ByDeviceID))

	for _, vol := range volumes {
		// Try to resolve device name from device ID
		if vol.DeviceID != "" {
			if s, ok := stats.ByDeviceID[vol.DeviceID]; ok {
				slog.Debug("resolved device", "deviceID", vol.DeviceID, "deviceName", s.DeviceName)
				vol.DeviceName = s.DeviceName
			} else {
				slog.Debug("no diskstats match", "deviceID", vol.DeviceID, "pvc", vol.PVCName)
			}
		} else {
			slog.Debug("no deviceID for volume", "pvc", vol.PVCName, "deviceName", vol.DeviceName)
		}
	}
}
