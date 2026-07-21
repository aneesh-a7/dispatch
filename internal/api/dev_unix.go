//go:build !windows

package api

import (
	"fmt"
	"os/exec"
	"runtime"
)

// openWorkerTerminal opens a new terminal window running a worker. macOS
// drives Terminal.app through osascript; elsewhere it asks the
// freedesktop-standard x-terminal-emulator to run a shell that starts the
// worker and stays open. These are best-effort: the project's primary
// target is Windows, and a machine without either mechanism will get a
// clear error surfaced back to the dashboard.
func openWorkerTerminal(dir string) error {
	switch runtime.GOOS {
	case "darwin":
		script := fmt.Sprintf(`tell application "Terminal" to do script "cd '%s' && go run ./cmd/worker"`, dir)
		return exec.Command("osascript", "-e", script).Start()
	default:
		inner := fmt.Sprintf("cd '%s' && go run ./cmd/worker; exec $SHELL", dir)
		return exec.Command("x-terminal-emulator", "-e", "sh", "-c", inner).Start()
	}
}
