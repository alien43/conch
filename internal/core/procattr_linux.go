package core

import "syscall"

// childProcAttr puts a child in its own process group and has the kernel
// SIGKILL it if the wrapper dies without cleaning up (SIGKILL, OOM kill).
//
// Pdeathsig fires when the *thread* that forked the child exits, not the
// process. Go only retires OS threads whose goroutine called LockOSThread and
// exited, so cmd.Start must never run under LockOSThread.
func childProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
}
