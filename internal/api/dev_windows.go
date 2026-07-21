//go:build windows

package api

import (
	"fmt"
	"os/exec"
	"syscall"
)

// openWorkerTerminal opens a new console window running a worker.
//
// The command line is built by hand and passed to CreateProcess through
// SysProcAttr.CmdLine instead of exec.Command's argument slice. cmd.exe's
// quoting rules (especially around the `start` builtin) do not match Go's
// default argument escaping, so writing the line ourselves is the
// reliable way to get the nested quoting right.
//
//	cmd /c start ""       open a new window; the empty "" is its title, so
//	                      start does not mistake the next token for one
//	/D "<dir>"            set the new window's working directory
//	cmd /k "go run ..."   run the worker and keep the window open after it
//	                      exits, so a compile error stays on screen instead
//	                      of the window vanishing
func openWorkerTerminal(dir string) error {
	cmd := exec.Command("cmd")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CmdLine: fmt.Sprintf(`cmd /c start "" /D "%s" cmd /k "go run ./cmd/worker"`, dir),
	}
	return cmd.Start()
}
