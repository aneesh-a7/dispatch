package sysinfo

import (
	"runtime"
	"testing"
)

func TestDetectCPU(t *testing.T) {
	if got := DetectCPU(); got < 1 {
		t.Errorf("DetectCPU() = %d, want at least 1", got)
	}
}

// This asserts the shape of the answer rather than a specific number,
// since the number depends on whatever machine is running the test. The
// bounds are wide on purpose: the failure worth catching is a unit mixup
// (bytes or kB reported as MB), which misses by three orders of
// magnitude, not a few hundred MB either way.
func TestDetectMemoryMB(t *testing.T) {
	mb, ok := DetectMemoryMB()
	if !ok {
		t.Skipf("memory detection is not implemented on %s", runtime.GOOS)
	}
	if mb < 256 {
		t.Errorf("DetectMemoryMB() = %d MB, implausibly small (unit bug?)", mb)
	}
	if mb > 64*1024*1024 {
		t.Errorf("DetectMemoryMB() = %d MB, implausibly large (unit bug?)", mb)
	}
}
