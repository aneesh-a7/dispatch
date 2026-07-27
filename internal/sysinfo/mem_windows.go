//go:build windows

package sysinfo

import (
	"syscall"
	"unsafe"
)

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX struct. The Length
// field has to be set to the struct's own size before the call: that is
// how the API versions itself, and it fails outright if the value is
// wrong.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// DetectMemoryMB asks kernel32 for total physical RAM.
//
// This goes through syscall rather than golang.org/x/sys/windows to keep
// the dependency count at zero, which is a standing constraint for this
// project and the reason the release builds cross-compile from one Linux
// runner with nothing installed.
func DetectMemoryMB() (int, bool) {
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return 0, false
	}
	proc, err := kernel32.FindProc("GlobalMemoryStatusEx")
	if err != nil {
		return 0, false
	}

	var status memoryStatusEx
	status.Length = uint32(unsafe.Sizeof(status))
	ret, _, _ := proc.Call(uintptr(unsafe.Pointer(&status)))
	if ret == 0 || status.TotalPhys == 0 {
		return 0, false
	}
	return int(status.TotalPhys / (1024 * 1024)), true
}
