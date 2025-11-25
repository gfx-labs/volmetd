package collector

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gfx-labs/volmetd/pkg/discovery"
	"github.com/gfx-labs/volmetd/pkg/mounts"
)

var capacityMetrics = MetricSet[*mounts.Capacity]{
	Gauge("capacity_bytes_total", "Total capacity in bytes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.TotalBytes) }),
	Gauge("capacity_bytes_used", "Used capacity in bytes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.UsedBytes) }),
	Gauge("capacity_bytes_free", "Free capacity in bytes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.FreeBytes) }),
	Gauge("capacity_inodes_total", "Total number of inodes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.TotalInodes) }),
	Gauge("capacity_inodes_used", "Used number of inodes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.UsedInodes) }),
	Gauge("capacity_inodes_free", "Free number of inodes", volumeLabels_, func(c *mounts.Capacity) float64 { return float64(c.FreeInodes) }),
}

// CapacityCollector collects filesystem capacity metrics via statfs
type CapacityCollector struct{}

// NewCapacityCollector creates a new capacity collector
func NewCapacityCollector() *CapacityCollector {
	return &CapacityCollector{}
}

func (c *CapacityCollector) Name() string {
	return "capacity"
}

func (c *CapacityCollector) Update(volumes []*discovery.VolumeInfo, ch chan<- prometheus.Metric) error {
	wg := sync.WaitGroup{}
	for _, vol := range volumes {
		if vol.MountPath == "" {
			continue
		}
		wg.Add(1)
		go func(vol *discovery.VolumeInfo) {
			defer wg.Done()
			if cap, err := mounts.GetCapacity(vol.MountPath); err == nil {
				capacityMetrics.Collect(cap, volumeLabels(vol), ch)
			}
		}(vol)
	}
	wg.Wait()

	return nil
}
