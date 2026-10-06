//go:build !linux

package core

import "syscall"

// childProcAttr puts a child in its own process group. Outside Linux there is
// no parent-death signal, so a SIGKILLed wrapper leaves its child running.
func childProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid: true,
	}
}
