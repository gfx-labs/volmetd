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

// ResolveDevice resolves a device path (following symlinks) and returns both
// the resolved path and the device name for diskstats
func ResolveDevice(devicePath string) (resolvedPath, deviceName string) {
	// Try to fully resolve symlinks
	resolved, err := evalSymlinks(devicePath)
	if err != nil {
		resolved = devicePath
	}

	// Extract device name (basename)
	parts := strings.Split(resolved, "/")
	name := parts[len(parts)-1]

	return resolved, name
}

// GetDeviceID returns the major:minor device ID for a mount point
// This works by stat'ing the mount point and extracting the device ID
func GetDeviceID(mountPoint string) (string, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(mountPoint, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", mountPoint, err)
	}

	// Dev contains major:minor encoded
	// On Linux: major = (dev >> 8) & 0xfff, minor = (dev & 0xff) | ((dev >> 12) & 0xfff00)
	major := (stat.Dev >> 8) & 0xfff
	minor := (stat.Dev & 0xff) | ((stat.Dev >> 12) & 0xfff00)

	return fmt.Sprintf("%d:%d", major, minor), nil
}

// evalSymlinks resolves all symlinks in a path
func evalSymlinks(path string) (string, error) {
	// Use filepath.EvalSymlinks equivalent
	for i := 0; i < 255; i++ { // limit iterations to prevent infinite loops
		fi, err := os.Lstat(path)
		if err != nil {
			return path, err
		}

		if fi.Mode()&os.ModeSymlink == 0 {
			return path, nil
		}

		target, err := os.Readlink(path)
		if err != nil {
			return path, err
		}

		if !strings.HasPrefix(target, "/") {
			// Relative symlink - resolve relative to parent dir
			dir := path[:strings.LastIndex(path, "/")+1]
			path = dir + target
		} else {
			path = target
		}
	}

	return path, fmt.Errorf("too many symlinks")
}

// GetDeviceName extracts the base device name from a device path
// e.g., /dev/sda1 -> sda1, /dev/mapper/foo -> dm-X (via symlink resolution)
func GetDeviceName(devicePath string) (string, error) {
	_, name := ResolveDevice(devicePath)
	return name, nil
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
