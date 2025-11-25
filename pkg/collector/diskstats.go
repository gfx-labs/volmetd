package collector

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gfx-labs/volmetd/pkg/discovery"
	"github.com/gfx-labs/volmetd/pkg/diskstats"
)

var volumeLabels_ = []string{
	"device",
	"pvc",
	"namespace",
	"pv",
	"pod",
	"pod_namespace",
	"storage_class",
	"csi_driver",
}

var diskstatsMetrics = MetricSet[*diskstats.Stats]{
	// Reads
	Counter("reads_completed_total", "Total number of reads completed successfully", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.ReadsCompleted) }),
	Counter("reads_merged_total", "Total number of reads merged", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.ReadsMerged) }),
	Counter("read_bytes_total", "Total number of bytes read", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.ReadBytesTotal()) }),
	Counter("read_time_seconds_total", "Total time spent reading in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.ReadTimeMs) / 1000 }),

	// Writes
	Counter("writes_completed_total", "Total number of writes completed successfully", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.WritesCompleted) }),
	Counter("writes_merged_total", "Total number of writes merged", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.WritesMerged) }),
	Counter("write_bytes_total", "Total number of bytes written", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.WriteBytesTotal()) }),
	Counter("write_time_seconds_total", "Total time spent writing in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.WriteTimeMs) / 1000 }),

	// I/O
	Gauge("io_in_progress", "Number of I/O operations currently in progress", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.IOInProgress) }),
	Counter("io_time_seconds_total", "Total time spent doing I/O in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.IOTimeMs) / 1000 }),
	Counter("weighted_io_time_seconds_total", "Weighted time spent doing I/O in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.WeightedIOTimeMs) / 1000 }),

	// Discards
	Counter("discards_completed_total", "Total number of discards completed successfully", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.DiscardsCompleted) }),
	Counter("discards_merged_total", "Total number of discards merged", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.DiscardsMerged) }),
	Counter("discard_bytes_total", "Total number of bytes discarded", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.SectorsDiscarded * 512) }),
	Counter("discard_time_seconds_total", "Total time spent discarding in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.DiscardTimeMs) / 1000 }),

	// Flushes
	Counter("flushes_completed_total", "Total number of flushes completed successfully", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.FlushCompleted) }),
	Counter("flush_time_seconds_total", "Total time spent flushing in seconds", volumeLabels_, func(s *diskstats.Stats) float64 { return float64(s.FlushTimeMs) / 1000 }),
}

// DiskstatsCollector collects disk I/O metrics from /proc/diskstats
type DiskstatsCollector struct {
	procPath string
}

// NewDiskstatsCollector creates a new diskstats collector
func NewDiskstatsCollector(procPath string) *DiskstatsCollector {
	if procPath == "" {
		procPath = "/proc"
	}
	return &DiskstatsCollector{procPath: procPath}
}

func (d *DiskstatsCollector) Name() string {
	return "diskstats"
}

func (d *DiskstatsCollector) Update(volumes []*discovery.VolumeInfo, ch chan<- prometheus.Metric) error {
	stats, err := diskstats.Parse(d.procPath + "/diskstats")
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, vol := range volumes {
		s, ok := stats[vol.DeviceName]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(vol *discovery.VolumeInfo, s *diskstats.Stats) {
			defer wg.Done()
			diskstatsMetrics.Collect(s, volumeLabels(vol), ch)
		}(vol, s)
	}
	wg.Wait()

	return nil
}

func volumeLabels(vol *discovery.VolumeInfo) []string {
	return []string{
		vol.DeviceName,
		vol.PVCName,
		vol.PVCNamespace,
		vol.PVName,
		vol.PodName,
		vol.PodNamespace,
		vol.StorageClass,
		vol.CSIDriver,
	}
}
