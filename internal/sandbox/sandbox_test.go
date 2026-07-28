package sandbox

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/aneesh/dispatch/internal/types"
)

func newTestSandbox(t *testing.T, opts Options) *Sandbox {
	t.Helper()
	sb, err := New("job_test_1", types.Resources{}, opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(sb.Close)
	return sb
}

// The whole reason this package exists. A worker started with a token in
// its environment was handing that token to every job it ran, which
// quietly turned "may submit a job" into "may keep the cluster
// credential."
func TestEnvironment_ExcludesSecrets(t *testing.T) {
	t.Setenv("DISPATCH_TOKEN", "super-secret-value")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "also-secret")

	sb := newTestSandbox(t, Options{Enabled: true})
	env := strings.Join(sb.environment(), "\n")

	for _, secret := range []string{"super-secret-value", "also-secret"} {
		if strings.Contains(env, secret) {
			t.Errorf("job environment leaked %q", secret)
		}
	}
	if strings.Contains(env, "DISPATCH_TOKEN") {
		t.Error("job environment contains DISPATCH_TOKEN")
	}
}

// An allowlist that strips everything is secure and useless: a program
// still has to be able to find its own interpreter.
func TestEnvironment_KeepsEssentials(t *testing.T) {
	sb := newTestSandbox(t, Options{Enabled: true})
	env := sb.environment()

	if !hasVar(env, "PATH") {
		t.Error("PATH was stripped; nothing would be executable")
	}
	if runtime.GOOS == "windows" && !hasVar(env, "SystemRoot") {
		t.Error("SystemRoot was stripped; most Windows programs will not start")
	}
}

func TestEnvironment_PassEnvForwardsExtras(t *testing.T) {
	t.Setenv("MY_APP_ENDPOINT", "https://example.com")

	sb := newTestSandbox(t, Options{Enabled: true, PassEnv: []string{"MY_APP_ENDPOINT"}})
	if !hasVar(sb.environment(), "MY_APP_ENDPOINT") {
		t.Error("explicitly forwarded variable was not passed through")
	}
}

// The escape hatch must not become a way to spell "everything".
func TestEnvironment_PassEnvDoesNotLeakOthers(t *testing.T) {
	t.Setenv("MY_APP_ENDPOINT", "https://example.com")
	t.Setenv("DISPATCH_TOKEN", "super-secret-value")

	sb := newTestSandbox(t, Options{Enabled: true, PassEnv: []string{"MY_APP_ENDPOINT"}})
	if strings.Contains(strings.Join(sb.environment(), "\n"), "super-secret-value") {
		t.Error("forwarding one variable leaked another")
	}
}

func TestEnvironment_TempPointsAtJobDirectory(t *testing.T) {
	sb := newTestSandbox(t, Options{Enabled: true})
	env := sb.environment()

	var tmp string
	for _, kv := range env {
		if strings.HasPrefix(kv, "TMPDIR=") {
			tmp = strings.TrimPrefix(kv, "TMPDIR=")
		}
	}
	if tmp != sb.Dir() {
		t.Errorf("TMPDIR = %q, want the job's own directory %q", tmp, sb.Dir())
	}
}

func TestWorkDir_IsolatedAndRemovedOnClose(t *testing.T) {
	sb, err := New("job_test_2", types.Resources{}, Options{Enabled: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	dir := sb.Dir()
	if dir == "" {
		t.Fatal("no working directory was created")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("working directory does not exist: %v", err)
	}

	// A job's scratch files must not outlive it.
	if err := os.WriteFile(dir+string(os.PathSeparator)+"scratch.txt", []byte("x"), 0o600); err != nil {
		t.Fatalf("writing into the job directory: %v", err)
	}
	sb.Close()

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("working directory survived Close (err = %v)", err)
	}
}

// Two jobs on one worker must not share scratch space.
func TestWorkDir_UniquePerJob(t *testing.T) {
	a := newTestSandbox(t, Options{Enabled: true})
	b := newTestSandbox(t, Options{Enabled: true})
	if a.Dir() == b.Dir() {
		t.Errorf("two sandboxes share a working directory: %s", a.Dir())
	}
}

func TestDisabled_LeavesCommandAlone(t *testing.T) {
	t.Setenv("DISPATCH_TOKEN", "super-secret-value")

	sb := newTestSandbox(t, Options{Enabled: false})
	if sb.Dir() != "" {
		t.Error("a disabled sandbox created a working directory")
	}

	cmd := exec.Command("go", "version")
	sb.Apply(cmd)
	if cmd.Env != nil {
		t.Error("a disabled sandbox rewrote the command environment")
	}
	if cmd.Dir != "" {
		t.Error("a disabled sandbox set a working directory")
	}
}

func TestApply_SetsDirAndEnv(t *testing.T) {
	sb := newTestSandbox(t, Options{Enabled: true})

	cmd := exec.Command("go", "version")
	sb.Apply(cmd)

	if cmd.Dir != sb.Dir() {
		t.Errorf("cmd.Dir = %q, want %q", cmd.Dir, sb.Dir())
	}
	if len(cmd.Env) == 0 {
		t.Error("cmd.Env was not set, so the child would inherit everything")
	}
}

func TestClose_IsIdempotent(t *testing.T) {
	sb, err := New("job_test_3", types.Resources{}, Options{Enabled: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sb.Close()
	sb.Close() // must not panic or error
}

func TestDescribe(t *testing.T) {
	if got := Describe(false); !strings.Contains(got, "disabled") {
		t.Errorf("Describe(false) = %q, want it to say disabled", got)
	}
	got := Describe(true)
	for _, want := range []string{"environment allowlist", "isolated working directory"} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe(true) = %q, missing %q", got, want)
		}
	}
}

// hasVar matches case-insensitively because Windows environment variable
// names are case-insensitive and the casing that actually shows up in
// os.Environ() depends on how the process was launched: this machine
// reports SYSTEMROOT where the documentation says SystemRoot. A
// case-sensitive check here fails against working code.
func hasVar(env []string, name string) bool {
	prefix := strings.ToLower(name) + "="
	for _, kv := range env {
		if strings.HasPrefix(strings.ToLower(kv), prefix) {
			return true
		}
	}
	return false
}
