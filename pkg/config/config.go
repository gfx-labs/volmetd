package config

import (
	"os"
	"strings"
)

// Discovery method names
const (
	DiscoveryCSI    = "csi"
	DiscoveryK8sAPI = "k8sapi"
)

// DefaultDiscoveryMethods is the default order of discovery methods
var DefaultDiscoveryMethods = []string{DiscoveryCSI, DiscoveryK8sAPI}

// Config holds the application configuration
type Config struct {
	// HTTP server
	ListenAddr  string
	MetricsPath string

	// Paths (for running in containers with host mounts)
	HostProcPath string // /proc on host
	KubeletPath  string // /var/lib/kubelet on host

	// Filtering
	Namespaces []string // empty = all namespaces

	// Discovery methods in priority order
	DiscoveryMethods []string

}

// DefaultConfig returns the default configuration with auto-detected paths
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:       ":6060",
		MetricsPath:      "/metrics",
		HostProcPath:     detectProcPath(),
		KubeletPath:      detectKubeletPath(),
		Namespaces:       nil,
		DiscoveryMethods: DefaultDiscoveryMethods,
	}
}

// detectProcPath returns /host/proc if it exists (container), otherwise /proc
func detectProcPath() string {
	if _, err := os.Stat("/host/proc/diskstats"); err == nil {
		return "/host/proc"
	}
	return "/proc"
}

// detectKubeletPath returns the kubelet path, checking common mount points
func detectKubeletPath() string {
	candidates := []string{
		"/host/var/lib/kubelet",
		"/var/lib/kubelet",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p + "/pods"); err == nil {
			return p
		}
	}
	// Default to container path since that's the primary use case
	return "/host/var/lib/kubelet"
}

// FromEnv loads configuration from environment variables
func FromEnv() *Config {
	c := DefaultConfig()

	if v := os.Getenv("VOLMETD_LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := os.Getenv("VOLMETD_METRICS_PATH"); v != "" {
		c.MetricsPath = v
	}
	if v := os.Getenv("VOLMETD_HOST_PROC_PATH"); v != "" {
		c.HostProcPath = v
	}
	if v := os.Getenv("VOLMETD_KUBELET_PATH"); v != "" {
		c.KubeletPath = v
	}
	if v := os.Getenv("VOLMETD_NAMESPACES"); v != "" {
		c.Namespaces = parseList(v)
	}
	if v := os.Getenv("VOLMETD_DISCOVERY_METHODS"); v != "" {
		c.DiscoveryMethods = parseList(v)
	}

	return c
}

func parseList(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// DiskstatsPath returns the path to /proc/diskstats
func (c *Config) DiskstatsPath() string {
	return c.HostProcPath + "/diskstats"
}

// MountsPath returns the path to /proc/mounts
func (c *Config) MountsPath() string {
	return c.HostProcPath + "/mounts"
}
