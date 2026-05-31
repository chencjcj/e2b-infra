package procstats

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadHugepagePool returns the hugetlb pool total/free in bytes by parsing
// /proc/meminfo on every call (uncached, suitable for the pressure sampler's
// 100 ms-floor cadence). Hugepagesize is read from meminfo, not hardcoded,
// so 1 GiB pages work too. Returns (0, 0, nil) when no pool is configured.
func ReadHugepagePool() (totalBytes, freeBytes uint64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, fmt.Errorf("open /proc/meminfo: %w", err)
	}
	defer f.Close()

	var total, free, sizeKB uint64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "HugePages_Total:"):
			if _, perr := fmt.Sscanf(line, "HugePages_Total: %d", &total); perr != nil {
				return 0, 0, fmt.Errorf("parse HugePages_Total: %w", perr)
			}
		case strings.HasPrefix(line, "HugePages_Free:"):
			if _, perr := fmt.Sscanf(line, "HugePages_Free: %d", &free); perr != nil {
				return 0, 0, fmt.Errorf("parse HugePages_Free: %w", perr)
			}
		case strings.HasPrefix(line, "Hugepagesize:"):
			if _, perr := fmt.Sscanf(line, "Hugepagesize: %d kB", &sizeKB); perr != nil {
				return 0, 0, fmt.Errorf("parse Hugepagesize: %w", perr)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("scan /proc/meminfo: %w", err)
	}

	return total * sizeKB * 1024, free * sizeKB * 1024, nil
}
