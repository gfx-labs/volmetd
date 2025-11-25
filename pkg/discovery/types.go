package discovery

import "context"

// VolumeInfo represents a discovered PVC volume
type VolumeInfo struct {
	// Kubernetes identifiers
	PVCName      string
	PVCNamespace string
	PVName       string

	// Pod info
	PodName      string
	PodNamespace string
	PodUID       string

	// Storage info
	StorageClass string
	CSIDriver    string
	VolumeHandle string // CSI volume handle / cloud provider volume ID

	// Node-local info
	DevicePath         string // e.g., /dev/sda
	DeviceName         string // e.g., sda
	MountPath          string // host path, e.g., /var/lib/kubelet/pods/.../volumes/...
	ContainerMountPath string // path inside container, e.g., /data
}

// Discoverer discovers PVC to device mappings
type Discoverer interface {
	// Name returns the discoverer name for logging
	Name() string

	// Discover returns all PVC volumes on this node
	Discover(ctx context.Context) ([]*VolumeInfo, error)

	// Available returns true if this discoverer can be used
	Available(ctx context.Context) bool
}

// MultiDiscoverer tries multiple discoverers and merges results
type MultiDiscoverer struct {
	discoverers []Discoverer
}

// NewMultiDiscoverer creates a new multi-discoverer
func NewMultiDiscoverer(discoverers ...Discoverer) *MultiDiscoverer {
	return &MultiDiscoverer{discoverers: discoverers}
}

// Discover tries all discoverers and returns merged results
func (m *MultiDiscoverer) Discover(ctx context.Context) ([]*VolumeInfo, error) {
	seen := make(map[string]*VolumeInfo) // key by device path

	for _, d := range m.discoverers {
		if !d.Available(ctx) {
			continue
		}

		volumes, err := d.Discover(ctx)
		if err != nil {
			// Log but continue with other discoverers
			continue
		}

		for _, v := range volumes {
			if v.DevicePath == "" {
				continue
			}
			// Prefer earlier discoverers (more authoritative)
			if _, exists := seen[v.DevicePath]; !exists {
				seen[v.DevicePath] = v
			}
		}
	}

	result := make([]*VolumeInfo, 0, len(seen))
	for _, v := range seen {
		result = append(result, v)
	}

	return result, nil
}
