//go:build linux

package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
)

// cgroupRoot is where cgroup v2 is conventionally mounted. If it is not
// there (v1-only systems, or an unusual mount layout), resource limits
// are skipped and the namespace isolation still applies.
const cgroupRoot = "/sys/fs/cgroup"

type platformState struct {
	cgroupDir string
}

func (s *Sandbox) initPlatform() error {
	s.platform = &platformState{}

	// Resource caps need a cgroup. Only bother when the job actually
	// asked for a limit; an unconstrained job gets namespaces only.
	if s.limits.Memory <= 0 && s.limits.CPU <= 0 {
		return nil
	}
	dir := filepath.Join(cgroupRoot, "dispatch-"+sanitizeID(s.jobID))
	if err := os.Mkdir(dir, 0o755); err != nil {
		// Not fatal. Running without a memory cap is worse than running
		// with one, but far better than refusing to run the job because
		// the host is not set up the way this code hoped.
		return nil
	}
	s.platform.cgroupDir = dir

	if s.limits.Memory > 0 {
		bytes := int64(s.limits.Memory) * 1024 * 1024
		writeCgroupFile(dir, "memory.max", strconv.FormatInt(bytes, 10))
		// Deny the swap escape hatch: without this, a job over its memory
		// cap gets pushed to swap and crawls instead of failing, which is
		// harder to diagnose than an honest OOM kill.
		writeCgroupFile(dir, "memory.swap.max", "0")
	}
	if s.limits.CPU > 0 {
		// cpu.max is "<quota> <period>" in microseconds. One CPU unit is
		// one core's worth of time.
		writeCgroupFile(dir, "cpu.max", strconv.Itoa(s.limits.CPU*100000)+" 100000")
	}
	return nil
}

func (s *Sandbox) applyPlatform(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Namespaces the job cannot see out of:
	//   NEWPID  its own PID 1, cannot signal the worker or its siblings
	//   NEWNS   mount changes stay inside the job
	//   NEWIPC  no shared memory or semaphores with anything else
	//   NEWUTS  cannot rename the host
	//
	// Network is deliberately NOT isolated. Most batch jobs exist to
	// fetch or upload something, and handing them a namespace with no
	// route out would break the common case to defend against the rare
	// one. Anyone who wants that should run the worker itself inside a
	// network namespace, which composes better than deciding here.
	cmd.SysProcAttr.Cloneflags |= syscall.CLONE_NEWPID |
		syscall.CLONE_NEWNS |
		syscall.CLONE_NEWIPC |
		syscall.CLONE_NEWUTS

	// Put the child in its own process group so a runaway that spawns
	// children can be killed as a unit rather than leaving orphans.
	cmd.SysProcAttr.Setpgid = true
}

func (s *Sandbox) startedPlatform(cmd *exec.Cmd) error {
	if s.platform == nil || s.platform.cgroupDir == "" {
		return nil
	}
	pid := strconv.Itoa(cmd.Process.Pid)
	if err := os.WriteFile(filepath.Join(s.platform.cgroupDir, "cgroup.procs"), []byte(pid), 0o644); err != nil {
		return fmt.Errorf("sandbox: adding job to cgroup: %w", err)
	}
	return nil
}

func (s *Sandbox) closePlatform() {
	if s.platform == nil || s.platform.cgroupDir == "" {
		return
	}
	// A cgroup can only be removed once empty, which it will be after the
	// process has exited. A failure here is not worth surfacing: the
	// directory is tiny and the next run uses a different job ID.
	os.Remove(s.platform.cgroupDir)
}

func platformFeatures() []string {
	return []string{
		"PID/mount/IPC/UTS namespaces",
		"cgroup v2 memory and CPU limits (when the job declares them)",
	}
}

func writeCgroupFile(dir, name, value string) {
	// Best-effort throughout: kernels differ in which controllers are
	// enabled, and a missing knob should cost that one limit rather than
	// the whole job.
	_ = os.WriteFile(filepath.Join(dir, name), []byte(value), 0o644)
}

// sanitizeID keeps a job ID usable as a directory name. IDs are already
// generated from a fixed alphabet, so this is belt and braces against a
// future ID format rather than a live concern.
func sanitizeID(id string) string {
	out := make([]rune, 0, len(id))
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
