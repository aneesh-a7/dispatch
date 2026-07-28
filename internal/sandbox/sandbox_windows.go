//go:build windows

package sandbox

import (
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"
)

// Windows has no namespaces, but it has Job Objects, which cover the two
// things that matter most here: a hard memory ceiling, and killing every
// process in the job when the handle closes. That second property is the
// answer to a job that spawns children and exits: without it, those
// children outlive the job and nothing is tracking them.

var (
	kernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObjectW      = kernel32.NewProc("CreateJobObjectW")
	procSetInformationJobObj  = kernel32.NewProc("SetInformationJobObject")
	procAssignProcessToJobObj = kernel32.NewProc("AssignProcessToJobObject")
	procOpenProcess           = kernel32.NewProc("OpenProcess")
	procCloseHandle           = kernel32.NewProc("CloseHandle")
)

const (
	jobObjectExtendedLimitInformation = 9

	jobObjectLimitProcessMemory  = 0x00000100
	jobObjectLimitJobMemory      = 0x00000200
	jobObjectLimitKillOnJobClose = 0x00002000
	jobObjectLimitActiveProcess  = 0x00000008

	processSetQuota  = 0x0100
	processTerminate = 0x0001
	processAllAccess = processSetQuota | processTerminate
)

type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

type jobObjectExtendedLimitInfo struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}

type platformState struct {
	job syscall.Handle
}

func (s *Sandbox) initPlatform() error {
	s.platform = &platformState{}

	h, _, err := procCreateJobObjectW.Call(0, 0)
	if h == 0 {
		// No job object means no memory ceiling and no kill-on-close, but
		// the portable protections still apply, so run the job anyway.
		return nil
	}
	job := syscall.Handle(h)

	info := jobObjectExtendedLimitInfo{}
	// Kill everything in the job when the last handle closes. This is the
	// property that makes a job's process tree actually disappear when
	// the job ends, rather than leaving grandchildren behind.
	info.BasicLimitInformation.LimitFlags = jobObjectLimitKillOnJobClose

	if s.limits.Memory > 0 {
		bytes := uintptr(s.limits.Memory) * 1024 * 1024
		info.ProcessMemoryLimit = bytes
		info.JobMemoryLimit = bytes
		info.BasicLimitInformation.LimitFlags |= jobObjectLimitProcessMemory | jobObjectLimitJobMemory
	}

	ret, _, _ := procSetInformationJobObj.Call(
		uintptr(job),
		jobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		unsafe.Sizeof(info),
	)
	if ret == 0 {
		procCloseHandle.Call(uintptr(job))
		_ = err
		return nil
	}
	s.platform.job = job
	return nil
}

func (s *Sandbox) applyPlatform(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Give the child its own process group so the whole tree can be
	// signalled together, mirroring the Setpgid call on Linux.
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
}

func (s *Sandbox) startedPlatform(cmd *exec.Cmd) error {
	if s.platform == nil || s.platform.job == 0 {
		return nil
	}
	h, _, _ := procOpenProcess.Call(processAllAccess, 0, uintptr(cmd.Process.Pid))
	if h == 0 {
		return fmt.Errorf("sandbox: opening process %d to assign it to a job object", cmd.Process.Pid)
	}
	defer procCloseHandle.Call(h)

	ret, _, _ := procAssignProcessToJobObj.Call(uintptr(s.platform.job), h)
	if ret == 0 {
		return fmt.Errorf("sandbox: assigning process %d to job object", cmd.Process.Pid)
	}
	return nil
}

func (s *Sandbox) closePlatform() {
	if s.platform == nil || s.platform.job == 0 {
		return
	}
	// Closing the last handle is what triggers kill-on-close, so this
	// doubles as the cleanup for any process the job left running.
	procCloseHandle.Call(uintptr(s.platform.job))
	s.platform.job = 0
}

func platformFeatures() []string {
	return []string{
		"job object with kill-on-close",
		"job object memory ceiling (when the job declares one)",
	}
}
