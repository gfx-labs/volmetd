package diskstats

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Stats represents disk I/O statistics from /proc/diskstats
// See https://www.kernel.org/doc/Documentation/iostats.txt
type Stats struct {
	Major       int
	Minor       int
	DeviceName  string

	// Reads
	ReadsCompleted  uint64
	ReadsMerged     uint64
	SectorsRead     uint64
	ReadTimeMs      uint64

	// Writes
	WritesCompleted uint64
	WritesMerged    uint64
	SectorsWritten  uint64
	WriteTimeMs     uint64

	// I/O
	IOInProgress    uint64
	IOTimeMs        uint64
	WeightedIOTimeMs uint64

	// Discards (kernel 4.18+)
	DiscardsCompleted uint64
	DiscardsMerged    uint64
	SectorsDiscarded  uint64
	DiscardTimeMs     uint64

	// Flush (kernel 5.5+)
	FlushCompleted uint64
	FlushTimeMs    uint64
}

// ReadBytesTotal returns total bytes read (sectors * 512)
func (s *Stats) ReadBytesTotal() uint64 {
	return s.SectorsRead * 512
}

// WriteBytesTotal returns total bytes written (sectors * 512)
func (s *Stats) WriteBytesTotal() uint64 {
	return s.SectorsWritten * 512
}

// StatsMap holds diskstats indexed by both device name and major:minor
type StatsMap struct {
	ByName     map[string]*Stats // keyed by device name (e.g., "sda")
	ByDeviceID map[string]*Stats // keyed by "major:minor" (e.g., "8:0")
}

// Parse reads /proc/diskstats and returns stats for all devices
func Parse(path string) (*StatsMap, error) {
	if path == "" {
		path = "/proc/diskstats"
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open diskstats: %w", err)
	}
	defer f.Close()

	result := &StatsMap{
		ByName:     make(map[string]*Stats),
		ByDeviceID: make(map[string]*Stats),
	}
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		stats, err := parseLine(scanner.Text())
		if err != nil {
			continue // skip malformed lines
		}
		result.ByName[stats.DeviceName] = stats
		deviceID := fmt.Sprintf("%d:%d", stats.Major, stats.Minor)
		result.ByDeviceID[deviceID] = stats
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan diskstats: %w", err)
	}

	return result, nil
}

func parseLine(line string) (*Stats, error) {
	fields := strings.Fields(line)
	if len(fields) < 14 {
		return nil, fmt.Errorf("not enough fields: %d", len(fields))
	}

	s := &Stats{}
	var err error

	s.Major, err = strconv.Atoi(fields[0])
	if err != nil {
		return nil, err
	}
	s.Minor, err = strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}
	s.DeviceName = fields[2]

	// Parse numeric fields
	nums := make([]uint64, len(fields)-3)
	for i := 3; i < len(fields); i++ {
		nums[i-3], err = strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return nil, err
		}
	}

	s.ReadsCompleted = nums[0]
	s.ReadsMerged = nums[1]
	s.SectorsRead = nums[2]
	s.ReadTimeMs = nums[3]
	s.WritesCompleted = nums[4]
	s.WritesMerged = nums[5]
	s.SectorsWritten = nums[6]
	s.WriteTimeMs = nums[7]
	s.IOInProgress = nums[8]
	s.IOTimeMs = nums[9]
	s.WeightedIOTimeMs = nums[10]

	// Discard stats (kernel 4.18+)
	if len(nums) >= 15 {
		s.DiscardsCompleted = nums[11]
		s.DiscardsMerged = nums[12]
		s.SectorsDiscarded = nums[13]
		s.DiscardTimeMs = nums[14]
	}

	// Flush stats (kernel 5.5+)
	if len(nums) >= 17 {
		s.FlushCompleted = nums[15]
		s.FlushTimeMs = nums[16]
	}

	return s, nil
}
