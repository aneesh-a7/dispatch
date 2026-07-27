//go:build !linux && !windows && !darwin

package sysinfo

// DetectMemoryMB has no implementation on this platform. Returning false
// rather than a guess lets the worker fall back to its documented default
// and say so, instead of advertising a number nobody chose.
func DetectMemoryMB() (int, bool) {
	return 0, false
}
