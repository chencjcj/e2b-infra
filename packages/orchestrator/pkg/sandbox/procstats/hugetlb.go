// Package procstats parses /proc files for sandbox-scoped host-side stats.
package procstats

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// HugetlbStats splits hugetlb by Private (freeable by SIGKILL) and Shared
// (memfd, survives the kill — other peers hold it).
type HugetlbStats struct {
	PrivateBytes uint64
	SharedBytes  uint64
}

func (s HugetlbStats) Total() uint64 {
	return s.PrivateBytes + s.SharedBytes
}

// ReadHugetlbStats parses /proc/<pid>/smaps_rollup. Missing file → zero, no err.
func ReadHugetlbStats(pid int) (HugetlbStats, error) {
	path := fmt.Sprintf("/proc/%d/smaps_rollup", pid)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HugetlbStats{}, nil
		}
		return HugetlbStats{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	return parseHugetlbRollup(f)
}

// ReadHugetlbBytes returns Private + Shared (perceived total). For
// freeable-by-kill accounting use ReadHugetlbStats.
func ReadHugetlbBytes(pid int) (uint64, error) {
	s, err := ReadHugetlbStats(pid)
	return s.Total(), err
}

func parseHugetlbRollup(r io.Reader) (HugetlbStats, error) {
	var stats HugetlbStats
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "Private_Hugetlb:"):
			v, perr := parseKBLine(line)
			if perr != nil {
				return HugetlbStats{}, fmt.Errorf("parse Private_Hugetlb: %w", perr)
			}
			stats.PrivateBytes = v * 1024
		case strings.HasPrefix(line, "Shared_Hugetlb:"):
			v, perr := parseKBLine(line)
			if perr != nil {
				return HugetlbStats{}, fmt.Errorf("parse Shared_Hugetlb: %w", perr)
			}
			stats.SharedBytes = v * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return HugetlbStats{}, fmt.Errorf("scan smaps_rollup: %w", err)
	}

	return stats, nil
}

func parseKBLine(line string) (uint64, error) {
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		return 0, fmt.Errorf("missing colon: %q", line)
	}
	fields := strings.Fields(line[colon+1:])
	if len(fields) == 0 {
		return 0, fmt.Errorf("no value: %q", line)
	}
	return strconv.ParseUint(fields[0], 10, 64)
}
