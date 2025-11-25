package discovery

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/gfx-labs/volmetd/pkg/mounts"
)

// K8sAPIDiscoverer discovers PVC volumes using the Kubernetes API
type K8sAPIDiscoverer struct {
	client      kubernetes.Interface
	nodeName    string
	kubeletPath string
	mountsPath  string
	namespaces  []string // empty = all namespaces
}

// ErrNotInCluster is returned when not running inside a Kubernetes cluster
var ErrNotInCluster = fmt.Errorf("not running in a kubernetes cluster")

// NewK8sAPIDiscoverer creates a new Kubernetes API discoverer
func NewK8sAPIDiscoverer(kubeletPath, mountsPath string, namespaces []string) (*K8sAPIDiscoverer, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		if rest.ErrNotInCluster == err {
			return nil, ErrNotInCluster
		}
		return nil, fmt.Errorf("k8s config: %w", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	nodeName := detectNodeName()
	log.Printf("k8sapi: detected node name: %q", nodeName)

	if kubeletPath == "" {
		kubeletPath = "/var/lib/kubelet"
	}
	if mountsPath == "" {
		mountsPath = "/proc/mounts"
	}

	return &K8sAPIDiscoverer{
		client:      client,
		nodeName:    nodeName,
		kubeletPath: kubeletPath,
		mountsPath:  mountsPath,
		namespaces:  namespaces,
	}, nil
}

// detectNodeName tries multiple methods to determine the node name
func detectNodeName() string {
	// 1. Explicit env var (standard k8s pattern)
	if v := os.Getenv("NODE_NAME"); v != "" {
		return v
	}

	// 2. Downward API file
	downwardPaths := []string{
		"/etc/podinfo/nodename",
		"/etc/hostname-node", // alternative mount point
	}
	for _, p := range downwardPaths {
		if data, err := os.ReadFile(p); err == nil {
			if name := strings.TrimSpace(string(data)); name != "" {
				return name
			}
		}
	}

	// 3. Hostname (works when hostNetwork: true or hostname matches node)
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}

	return ""
}

func (d *K8sAPIDiscoverer) Name() string {
	return "k8sapi"
}

func (d *K8sAPIDiscoverer) Available(ctx context.Context) bool {
	if d.client == nil {
		log.Printf("k8sapi: client is nil")
		return false
	}
	if d.nodeName == "" {
		log.Printf("k8sapi: node name not detected")
		return false
	}
	// Quick check that we can talk to the API
	_, err := d.client.CoreV1().Nodes().Get(ctx, d.nodeName, metav1.GetOptions{})
	if err != nil {
		log.Printf("k8sapi: cannot get node %s: %v", d.nodeName, err)
		return false
	}
	return true
}

func (d *K8sAPIDiscoverer) Discover(ctx context.Context) ([]*VolumeInfo, error) {
	allMounts, err := mounts.Parse(d.mountsPath)
	if err != nil {
		return nil, err
	}

	// Get all pods on this node
	pods, err := d.getPodsOnNode(ctx)
	if err != nil {
		return nil, err
	}
	log.Printf("k8sapi: found %d pods on node %s", len(pods), d.nodeName)

	// Build PV -> PVC mapping
	pvToPVC := make(map[string]*pvcInfo)
	pvs, err := d.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, pv := range pvs.Items {
			if pv.Spec.ClaimRef != nil {
				pvToPVC[pv.Name] = &pvcInfo{
					name:         pv.Spec.ClaimRef.Name,
					namespace:    pv.Spec.ClaimRef.Namespace,
					storageClass: pv.Spec.StorageClassName,
					csiDriver:    getCSIDriver(&pv),
					volumeHandle: getVolumeHandle(&pv),
				}
			}
		}
	}

	var volumes []*VolumeInfo

	for _, pod := range pods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}

			pvcName := vol.PersistentVolumeClaim.ClaimName
			pvcNamespace := pod.Namespace

			// Get the PVC
			pvc, err := d.client.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				continue
			}

			pvName := pvc.Spec.VolumeName
			if pvName == "" {
				continue
			}

			// Find mount path for this volume
			mountPath := d.findMountPath(string(pod.UID), vol.Name)
			if mountPath == "" {
				log.Printf("k8sapi: no mount path for pod=%s vol=%s pvc=%s", pod.Name, vol.Name, pvcName)
				continue
			}

			// Find device from mount
			mount := mounts.FindMountByPath(allMounts, mountPath)
			if mount == nil {
				log.Printf("k8sapi: no mount entry for path=%s", mountPath)
				continue
			}

			// Resolve symlinks to get actual device for diskstats
			resolvedPath, deviceName := mounts.ResolveDevice(mount.Device)

			// Get device ID from mount point for reliable diskstats lookup
			deviceID, _ := mounts.GetDeviceID(mountPath)

			// Find container mount path
			containerMountPath := findContainerMountPath(&pod, vol.Name)

			pvcMeta := pvToPVC[pvName]

			volInfo := &VolumeInfo{
				PVCName:            pvcName,
				PVCNamespace:       pvcNamespace,
				PVName:             pvName,
				PodName:            pod.Name,
				PodNamespace:       pod.Namespace,
				PodUID:             string(pod.UID),
				CSIDevicePath:      mount.Device,
				DevicePath:         resolvedPath,
				DeviceName:         deviceName,
				DeviceID:           deviceID,
				MountPath:          mountPath,
				ContainerMountPath: containerMountPath,
			}

			if pvcMeta != nil {
				volInfo.StorageClass = pvcMeta.storageClass
				volInfo.CSIDriver = pvcMeta.csiDriver
				volInfo.VolumeHandle = pvcMeta.volumeHandle
			}

			log.Printf("k8sapi: found volume pvc=%s/%s pv=%s deviceID=%s", pvcNamespace, pvcName, pvName, deviceID)
			volumes = append(volumes, volInfo)
		}
	}

	return volumes, nil
}

type pvcInfo struct {
	name         string
	namespace    string
	storageClass string
	csiDriver    string
	volumeHandle string
}

func (d *K8sAPIDiscoverer) getPodsOnNode(ctx context.Context) ([]corev1.Pod, error) {
	var allPods []corev1.Pod

	if len(d.namespaces) == 0 {
		// All namespaces
		pods, err := d.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + d.nodeName,
		})
		if err != nil {
			return nil, err
		}
		allPods = pods.Items
	} else {
		for _, ns := range d.namespaces {
			pods, err := d.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
				FieldSelector: "spec.nodeName=" + d.nodeName,
			})
			if err != nil {
				continue
			}
			allPods = append(allPods, pods.Items...)
		}
	}

	return allPods, nil
}

func (d *K8sAPIDiscoverer) findMountPath(podUID, volName string) string {
	// CSI volumes
	csiPath := filepath.Join(d.kubeletPath, "pods", podUID, "volumes", "kubernetes.io~csi", volName, "mount")
	if _, err := os.Stat(csiPath); err == nil {
		return csiPath
	}

	// Regular PV volumes
	pvPath := filepath.Join(d.kubeletPath, "pods", podUID, "volumes", "kubernetes.io~projected", volName)
	if _, err := os.Stat(pvPath); err == nil {
		return pvPath
	}

	return ""
}

func getCSIDriver(pv *corev1.PersistentVolume) string {
	if pv.Spec.CSI != nil {
		return pv.Spec.CSI.Driver
	}
	return ""
}

func getVolumeHandle(pv *corev1.PersistentVolume) string {
	if pv.Spec.CSI != nil {
		return pv.Spec.CSI.VolumeHandle
	}
	return ""
}

// findContainerMountPath finds the mount path inside containers for a volume
func findContainerMountPath(pod *corev1.Pod, volName string) string {
	// Check regular containers first
	for _, c := range pod.Spec.Containers {
		for _, vm := range c.VolumeMounts {
			if vm.Name == volName {
				return vm.MountPath
			}
		}
	}
	// Check init containers
	for _, c := range pod.Spec.InitContainers {
		for _, vm := range c.VolumeMounts {
			if vm.Name == volName {
				return vm.MountPath
			}
		}
	}
	return ""
}
