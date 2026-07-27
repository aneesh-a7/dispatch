// Package sysinfo detects how much machine a worker actually has, so its
// advertised capacity defaults to something true rather than something
// typed from memory.
//
// Getting this wrong is quietly bad: the scheduler bin-packs against
// whatever a worker claims, so a worker that overstates itself will
// cheerfully accept more work than it can run, and one that understates
// itself sits half-idle while jobs queue. Neither failure announces
// itself.
//
// Memory detection is per-platform and stdlib-only (no cgo, so the
// release builds stay cross-compilable). Where a platform is not
// supported, DetectMemoryMB returns false and the caller keeps its
// default rather than guessing.
package sysinfo

import "runtime"

// DetectCPU returns the number of usable cores. runtime.NumCPU already
// respects CPU affinity and container CPU limits on Linux, which is
// exactly the number a worker should be advertising.
func DetectCPU() int {
	return runtime.NumCPU()
}
