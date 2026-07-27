//go:build linux

package sysinfo

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// DetectMemoryMB reads total RAM from /proc/meminfo.
//
// It reports MemTotal rather than MemAvailable on purpose: capacity is
// meant to describe the machine, not its mood at the moment the worker
// happened to start. A worker that registered during a memory spike would
// otherwise advertise a small capacity and keep it for its whole life.
func DetectMemoryMB() (int, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		// "MemTotal:  16316196 kB"
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb <= 0 {
			return 0, false
		}
		return int(kb / 1024), true
	}
	return 0, false
}
