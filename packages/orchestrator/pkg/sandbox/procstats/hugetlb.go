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

// ReadHugetlbBytes returns Private_Hugetlb + Shared_Hugetlb from /proc/<pid>/smaps_rollup.
// (0, nil) when the file is missing (process exited / no hugetlb mappings).
func ReadHugetlbBytes(pid int) (uint64, error) {
	path := fmt.Sprintf("/proc/%d/smaps_rollup", pid)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	return parseHugetlbRollup(f)
}

func parseHugetlbRollup(r io.Reader) (uint64, error) {
	var privateKB, sharedKB uint64
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "Private_Hugetlb:"):
			v, perr := parseKBLine(line)
			if perr != nil {
				return 0, fmt.Errorf("parse Private_Hugetlb: %w", perr)
			}
			privateKB = v
		case strings.HasPrefix(line, "Shared_Hugetlb:"):
			v, perr := parseKBLine(line)
			if perr != nil {
				return 0, fmt.Errorf("parse Shared_Hugetlb: %w", perr)
			}
			sharedKB = v
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan smaps_rollup: %w", err)
	}

	return (privateKB + sharedKB) * 1024, nil
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
