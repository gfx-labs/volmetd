package mounts

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"
)

// Mount represents a mounted filesystem
type Mount struct {
	Device     string
	MountPoint string
	FSType     string
	Options    string
}

// Capacity represents filesystem capacity information
type Capacity struct {
	TotalBytes uint64
	UsedBytes  uint64
	FreeBytes  uint64
	TotalInodes uint64
	UsedInodes  uint64
	FreeInodes  uint64
}

// MountWithCapacity combines mount info with capacity stats
type MountWithCapacity struct {
	Mount
	Capacity
}

// Parse reads /proc/mounts and returns all mounts
func Parse(path string) ([]*Mount, error) {
	if path == "" {
		path = "/proc/mounts"
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mounts: %w", err)
	}
	defer f.Close()

	var mounts []*Mount
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		m, err := parseLine(scanner.Text())
		if err != nil {
			continue
		}
		mounts = append(mounts, m)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan mounts: %w", err)
	}

	return mounts, nil
}

func parseLine(line string) (*Mount, error) {
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return nil, fmt.Errorf("not enough fields")
	}

	return &Mount{
		Device:     fields[0],
		MountPoint: fields[1],
		FSType:     fields[2],
		Options:    fields[3],
	}, nil
}

// GetCapacity returns capacity information for a mount point
func GetCapacity(mountPoint string) (*Capacity, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mountPoint, &stat); err != nil {
		return nil, fmt.Errorf("statfs %s: %w", mountPoint, err)
	}

	blockSize := uint64(stat.Bsize)

	return &Capacity{
		TotalBytes:  stat.Blocks * blockSize,
		FreeBytes:   stat.Bfree * blockSize,
		UsedBytes:   (stat.Blocks - stat.Bfree) * blockSize,
		TotalInodes: stat.Files,
		FreeInodes:  stat.Ffree,
		UsedInodes:  stat.Files - stat.Ffree,
	}, nil
}

// GetDeviceName extracts the base device name from a device path
// e.g., /dev/sda1 -> sda1, /dev/mapper/foo -> dm-X (via symlink resolution)
func GetDeviceName(devicePath string) (string, error) {
	// Resolve symlinks
	resolved, err := os.Readlink(devicePath)
	if err != nil {
		// Not a symlink, use basename
		parts := strings.Split(devicePath, "/")
		return parts[len(parts)-1], nil
	}

	// If relative path, it's relative to /dev
	if !strings.HasPrefix(resolved, "/") {
		resolved = "/dev/" + resolved
	}

	parts := strings.Split(resolved, "/")
	return parts[len(parts)-1], nil
}

// FindMountByPath finds a mount that contains the given path
func FindMountByPath(mounts []*Mount, path string) *Mount {
	var best *Mount
	bestLen := 0

	for _, m := range mounts {
		if strings.HasPrefix(path, m.MountPoint) {
			if len(m.MountPoint) > bestLen {
				best = m
				bestLen = len(m.MountPoint)
			}
		}
	}

	return best
}
