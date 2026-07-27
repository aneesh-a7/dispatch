//go:build darwin

package sysinfo

import (
	"encoding/binary"
	"syscall"
)

// DetectMemoryMB reads hw.memsize via sysctl.
//
// syscall.Sysctl returns the raw value as a string, and Go's own wrapper
// trims what it thinks is a trailing NUL. For hw.memsize the value is an
// 8-byte little-endian integer whose top byte is legitimately zero on
// every machine with less than 72PB of RAM, so the result usually arrives
// one byte short. Padding it back out is the fix.
func DetectMemoryMB() (int, bool) {
	raw, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0, false
	}
	buf := []byte(raw)
	if len(buf) == 0 || len(buf) > 8 {
		return 0, false
	}
	padded := make([]byte, 8)
	copy(padded, buf)

	total := binary.LittleEndian.Uint64(padded)
	if total == 0 {
		return 0, false
	}
	return int(total / (1024 * 1024)), true
}
