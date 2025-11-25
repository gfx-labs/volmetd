package discovery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/gfx-labs/volmetd/pkg/mounts"
)

// CSIDiscoverer discovers PVC volumes by parsing kubelet CSI volume directories
type CSIDiscoverer struct {
	kubeletPath string
	mountsPath  string
}

// NewCSIDiscoverer creates a new CSI discoverer
func NewCSIDiscoverer(kubeletPath, mountsPath string) *CSIDiscoverer {
	if kubeletPath == "" {
		kubeletPath = "/var/lib/kubelet"
	}
	if mountsPath == "" {
		mountsPath = "/proc/mounts"
	}
	return &CSIDiscoverer{
		kubeletPath: kubeletPath,
		mountsPath:  mountsPath,
	}
}

func (d *CSIDiscoverer) Name() string {
	return "csi"
}

func (d *CSIDiscoverer) Available(ctx context.Context) bool {
	podsDir := filepath.Join(d.kubeletPath, "pods")
	_, err := os.Stat(podsDir)
	return err == nil
}

func (d *CSIDiscoverer) Discover(ctx context.Context) ([]*VolumeInfo, error) {
	allMounts, err := mounts.Parse(d.mountsPath)
	if err != nil {
		return nil, err
	}

	podsDir := filepath.Join(d.kubeletPath, "pods")
	podDirs, err := os.ReadDir(podsDir)
	if err != nil {
		return nil, err
	}

	var volumes []*VolumeInfo

	for _, podDir := range podDirs {
		if !podDir.IsDir() {
			continue
		}

		podUID := podDir.Name()
		volumesDir := filepath.Join(podsDir, podUID, "volumes")

		if _, err := os.Stat(volumesDir); os.IsNotExist(err) {
			continue
		}

		// Check kubernetes.io~csi directory for CSI volumes
		csiDir := filepath.Join(volumesDir, "kubernetes.io~csi")
		if vols, err := d.discoverCSIVolumes(ctx, podUID, csiDir, allMounts); err == nil {
			volumes = append(volumes, vols...)
		}

		// Check for regular PV mounts
		pvDir := filepath.Join(volumesDir, "kubernetes.io~projected")
		if vols, err := d.discoverProjectedVolumes(ctx, podUID, pvDir, allMounts); err == nil {
			volumes = append(volumes, vols...)
		}
	}

	return volumes, nil
}

func (d *CSIDiscoverer) discoverCSIVolumes(ctx context.Context, podUID, csiDir string, allMounts []*mounts.Mount) ([]*VolumeInfo, error) {
	volDirs, err := os.ReadDir(csiDir)
	if err != nil {
		return nil, err
	}

	var volumes []*VolumeInfo

	for _, volDir := range volDirs {
		if !volDir.IsDir() {
			continue
		}

		volName := volDir.Name()
		volPath := filepath.Join(csiDir, volName)
		mountPath := filepath.Join(volPath, "mount")

		// Read vol_data.json for volume metadata
		volDataPath := filepath.Join(volPath, "vol_data.json")
		volData, err := d.readVolData(volDataPath)
		if err != nil {
			continue
		}

		// Find the device backing this mount
		mount := mounts.FindMountByPath(allMounts, mountPath)
		if mount == nil {
			continue
		}

		deviceName, _ := mounts.GetDeviceName(mount.Device)

		vol := &VolumeInfo{
			PVName:       volData.VolumeName,
			PVCName:      extractPVCName(volData.VolumeName),
			PVCNamespace: volData.PodNamespace,
			PodName:      volData.PodName,
			PodNamespace: volData.PodNamespace,
			PodUID:       podUID,
			CSIDriver:    volData.DriverName,
			VolumeHandle: volData.VolumeHandle,
			DevicePath:   mount.Device,
			DeviceName:   deviceName,
			MountPath:    mountPath,
		}

		volumes = append(volumes, vol)
	}

	return volumes, nil
}

func (d *CSIDiscoverer) discoverProjectedVolumes(ctx context.Context, podUID, pvDir string, allMounts []*mounts.Mount) ([]*VolumeInfo, error) {
	// Projected volumes are typically not block devices, skip for now
	return nil, nil
}

type volData struct {
	VolumeName   string `json:"specVolID"`
	DriverName   string `json:"driverName"`
	VolumeHandle string `json:"volumeHandle"`
	PodName      string `json:"kubernetes.io/pod.name"`
	PodNamespace string `json:"kubernetes.io/pod.namespace"`
	PodUID       string `json:"kubernetes.io/pod.uid"`
}

func (d *CSIDiscoverer) readVolData(path string) (*volData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Parse as generic map first since keys have dots
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	vd := &volData{}
	if v, ok := raw["specVolID"].(string); ok {
		vd.VolumeName = v
	}
	if v, ok := raw["driverName"].(string); ok {
		vd.DriverName = v
	}
	if v, ok := raw["volumeHandle"].(string); ok {
		vd.VolumeHandle = v
	}
	if v, ok := raw["kubernetes.io/pod.name"].(string); ok {
		vd.PodName = v
	}
	if v, ok := raw["kubernetes.io/pod.namespace"].(string); ok {
		vd.PodNamespace = v
	}
	if v, ok := raw["kubernetes.io/pod.uid"].(string); ok {
		vd.PodUID = v
	}

	return vd, nil
}

// extractPVCName tries to extract PVC name from PV name
// PV names are often like "pvc-<uuid>" but we need to look up the actual PVC
func extractPVCName(pvName string) string {
	// This is a placeholder - real PVC name needs K8s API lookup
	// For now return PV name
	if strings.HasPrefix(pvName, "pvc-") {
		return pvName
	}
	return pvName
}
