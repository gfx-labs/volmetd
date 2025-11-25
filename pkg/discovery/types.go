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
	DevicePath         string // resolved device path, e.g., /dev/sda
	DeviceName         string // device name for diskstats, e.g., sda
	CSIDevicePath      string // original CSI device path, e.g., /dev/disk/by-id/scsi-0DO_Volume_...
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
	seen := make(map[string]*VolumeInfo) // key by device name

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
			if v.DeviceName == "" {
				continue
			}
			if existing, exists := seen[v.DeviceName]; exists {
				// Merge: fill in empty fields from new discoverer
				mergeVolumeInfo(existing, v)
			} else {
				seen[v.DeviceName] = v
			}
		}
	}

	result := make([]*VolumeInfo, 0, len(seen))
	for _, v := range seen {
		result = append(result, v)
	}

	return result, nil
}

// mergeVolumeInfo fills empty fields in dst from src
func mergeVolumeInfo(dst, src *VolumeInfo) {
	if dst.PVCName == "" || dst.PVCName == dst.PVName {
		if src.PVCName != "" && src.PVCName != src.PVName {
			dst.PVCName = src.PVCName
		}
	}
	if dst.PVCNamespace == "" {
		dst.PVCNamespace = src.PVCNamespace
	}
	if dst.PVName == "" {
		dst.PVName = src.PVName
	}
	if dst.PodName == "" {
		dst.PodName = src.PodName
	}
	if dst.PodNamespace == "" {
		dst.PodNamespace = src.PodNamespace
	}
	if dst.PodUID == "" {
		dst.PodUID = src.PodUID
	}
	if dst.StorageClass == "" {
		dst.StorageClass = src.StorageClass
	}
	if dst.CSIDriver == "" {
		dst.CSIDriver = src.CSIDriver
	}
	if dst.VolumeHandle == "" {
		dst.VolumeHandle = src.VolumeHandle
	}
	if dst.DevicePath == "" {
		dst.DevicePath = src.DevicePath
	}
	if dst.CSIDevicePath == "" {
		dst.CSIDevicePath = src.CSIDevicePath
	}
	if dst.MountPath == "" {
		dst.MountPath = src.MountPath
	}
	if dst.ContainerMountPath == "" {
		dst.ContainerMountPath = src.ContainerMountPath
	}
}
