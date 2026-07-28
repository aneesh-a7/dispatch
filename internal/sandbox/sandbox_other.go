//go:build !linux && !windows

package sandbox

import "os/exec"

// Platforms without a specific implementation get the portable
// protections only. Saying so plainly (see platformFeatures) matters more
// than it looks: an operator who believes a memory cap is in force when
// it is not has been made worse off than one who knows there is none.

type platformState struct{}

func (s *Sandbox) initPlatform() error { return nil }

func (s *Sandbox) applyPlatform(cmd *exec.Cmd) {}

func (s *Sandbox) startedPlatform(cmd *exec.Cmd) error { return nil }

func (s *Sandbox) closePlatform() {}

func platformFeatures() []string {
	return []string{"no OS-level resource limits available on this platform"}
}
