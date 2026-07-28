// Package sandbox puts a fence around the subprocess a worker runs.
//
// Until this existed, a job was a plain os/exec child: it inherited the
// worker's entire environment, ran in the worker's own working directory,
// and could use as much memory as it liked. That is fine when every job
// is a script you wrote yourself, which is how this project started, and
// steadily less fine as soon as anyone else can submit work.
//
// The environment part was the sharp edge. A worker started with
// DISPATCH_TOKEN in its environment handed that token to every job it
// ran, so "may submit a job" silently implied "may read the cluster
// credential and keep it." An allowlist fixes that: a job gets the few
// variables a program genuinely needs to run and nothing else.
//
// What can actually be enforced varies by platform, and this package is
// deliberately honest about that rather than implying a guarantee it
// cannot keep. Describe() reports what is really in effect so the worker
// can log it at startup instead of leaving operators to guess.
package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aneesh/dispatch/internal/types"
)

// Options configures a sandbox.
type Options struct {
	// Enabled turns the whole thing off when false, restoring the old
	// behaviour of running jobs as plain inherited-environment children.
	Enabled bool

	// PassEnv names extra environment variables to forward beyond the
	// baseline allowlist. This is the escape hatch for jobs that
	// legitimately need something specific (an API endpoint, a proxy
	// setting) without reopening the whole environment.
	PassEnv []string
}

// Sandbox is the isolation applied to one job's subprocess. Create it
// with New, apply it before starting the command, and always Close it.
type Sandbox struct {
	enabled bool
	jobID   string
	workDir string
	limits  types.Resources
	passEnv []string

	// platform holds whatever the OS-specific layer needs to clean up.
	// It is nil where the platform has no extra machinery.
	platform *platformState
}

// New prepares a sandbox for a job. limits comes straight from the job's
// declared resource request, which is a detail worth noticing: the same
// numbers the scheduler uses to decide which worker a job fits on become
// the numbers the OS enforces once it lands. A job that asks for 512MB is
// both scheduled as needing 512MB and stopped at 512MB, instead of the
// request being an honour-system hint.
//
// A zero limit means unconstrained, matching how the scheduler already
// treats it.
func New(jobID string, limits types.Resources, opts Options) (*Sandbox, error) {
	s := &Sandbox{
		enabled: opts.Enabled,
		jobID:   jobID,
		limits:  limits,
		passEnv: opts.PassEnv,
	}
	if !s.enabled {
		return s, nil
	}

	// Each job gets its own working directory, so a job cannot litter in
	// (or read from) the worker's own directory, and two jobs running at
	// once on the same worker cannot collide over a scratch filename.
	dir, err := os.MkdirTemp("", "dispatch-job-")
	if err != nil {
		return nil, fmt.Errorf("sandbox: creating work directory: %w", err)
	}
	s.workDir = dir

	if err := s.initPlatform(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return s, nil
}

// Apply configures cmd to run inside the sandbox. Call it before starting
// the command.
func (s *Sandbox) Apply(cmd *exec.Cmd) {
	if !s.enabled {
		return
	}
	cmd.Dir = s.workDir
	cmd.Env = s.environment()
	s.applyPlatform(cmd)
}

// Started performs any isolation that can only happen once the child
// exists, such as moving it into a cgroup or a job object. Call it
// immediately after cmd.Start() succeeds.
//
// A failure here is reported but is not fatal to the job: the difference
// between "ran with fewer limits than intended" and "did not run" is a
// judgement call, and silently refusing to run work because a limit could
// not be applied would be its own kind of outage. The worker logs it so
// the gap is visible.
func (s *Sandbox) Started(cmd *exec.Cmd) error {
	if !s.enabled || cmd.Process == nil {
		return nil
	}
	return s.startedPlatform(cmd)
}

// Close releases everything the sandbox holds. Safe to call more than
// once, and safe to call on a sandbox that was never enabled.
func (s *Sandbox) Close() {
	if !s.enabled {
		return
	}
	s.closePlatform()
	if s.workDir != "" {
		os.RemoveAll(s.workDir)
	}
}

// Dir reports the isolated working directory, or "" when disabled.
func (s *Sandbox) Dir() string { return s.workDir }

// baselineEnv lists the variables a program needs before it can be said
// to run at all. This is an allowlist and not a denylist on purpose:
// a denylist has to anticipate every secret anyone might ever put in the
// worker's environment, and it only takes one it did not think of.
var baselineEnv = []string{
	// POSIX
	"PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR", "SHELL", "USER",
	// Windows: without SystemRoot and friends, a surprising amount of
	// the OS simply does not work, including parts of the Go runtime's
	// own process startup.
	"SystemRoot", "windir", "TEMP", "TMP", "COMSPEC", "PATHEXT",
	"NUMBER_OF_PROCESSORS", "OS", "USERPROFILE", "SystemDrive",
	"LOCALAPPDATA", "APPDATA", "ProgramData", "ProgramFiles",
}

// environment builds the child's environment from the allowlist plus any
// explicitly forwarded variables.
func (s *Sandbox) environment() []string {
	allowed := make(map[string]bool, len(baselineEnv)+len(s.passEnv))
	for _, k := range baselineEnv {
		allowed[strings.ToLower(k)] = true
	}
	for _, k := range s.passEnv {
		if k = strings.TrimSpace(k); k != "" {
			allowed[strings.ToLower(k)] = true
		}
	}

	var env []string
	for _, kv := range os.Environ() {
		i := strings.IndexByte(kv, '=')
		if i <= 0 {
			continue
		}
		if allowed[strings.ToLower(kv[:i])] {
			env = append(env, kv)
		}
	}
	// Point temp-file APIs at the job's own directory so a job that
	// writes scratch files gets them cleaned up with everything else.
	if s.workDir != "" {
		env = append(env, "TMPDIR="+s.workDir, "TEMP="+s.workDir, "TMP="+s.workDir)
	}
	return env
}

// Describe reports the isolation actually available here, for logging at
// worker startup. It names what is missing as well as what is present,
// since a sandbox nobody understands the limits of is worse than none.
func Describe(enabled bool) string {
	if !enabled {
		return "disabled (jobs inherit the worker's environment and run in its working directory)"
	}
	parts := append([]string{"environment allowlist", "isolated working directory"}, platformFeatures()...)
	return strings.Join(parts, ", ")
}

// scratchPath is a helper for platform code that needs a file path
// unique to this job.
func (s *Sandbox) scratchPath(name string) string {
	return filepath.Join(s.workDir, name)
}
