//go:build linux

package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"
)

func buildConch(t *testing.T) string {
	t.Helper()
	moduleRoot, err := findModuleRoot()
	if err != nil {
		t.Fatalf("failed to find module root: %v", err)
	}
	conchPath := filepath.Join(t.TempDir(), "conch")
	buildCmd := exec.Command("go", "build", "-o", conchPath, filepath.Join(moduleRoot, "cmd", "conch", "main.go"))
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build conch: %v\n%s", err, out)
	}
	return conchPath
}

// readPid waits for a pidfile to be written and returns its PID.
func readPid(t *testing.T, path string) int {
	t.Helper()
	for i := 0; i < 100; i++ {
		if b, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				return pid
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no pid in %s", path)
	return 0
}

func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// TestChildDiesWithKilledWrapper (H3): a wrapper that is SIGKILLed (or
// OOM-killed) must not leave its child running past the lease. The child
// under a wrapper that is left alone must keep running.
func TestChildDiesWithKilledWrapper(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()
	conch := buildConch(t)

	start := func(office, pidfile string) *exec.Cmd {
		cmd := exec.Command(conch, "elect", office, "--endpoints", etcd.ClientURL, "--ttl", "6s",
			"--", "sh", "-c", "echo $$ > "+pidfile+"; exec sleep 300")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start wrapper: %v", err)
		}
		return cmd
	}

	dir := t.TempDir()

	// Control: wrapper left alone, child must survive.
	keep := start("h3-keep", dir+"/keep.pid")
	defer func() {
		_ = keep.Process.Signal(syscall.SIGTERM)
		_ = keep.Wait()
	}()
	keepPid := readPid(t, dir+"/keep.pid")

	// Victim: wrapper SIGKILLed, child must die with it.
	victim := start("h3-kill", dir+"/kill.pid")
	childPid := readPid(t, dir+"/kill.pid")
	defer func() { _ = syscall.Kill(childPid, syscall.SIGKILL) }()

	if err := victim.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("failed to SIGKILL wrapper: %v", err)
	}
	_ = victim.Wait()

	gone := false
	for i := 0; i < 20; i++ {
		if !alive(childPid) {
			gone = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !gone {
		t.Errorf("child %d still running 2s after its wrapper was SIGKILLed", childPid)
	}

	time.Sleep(5 * time.Second)
	if !alive(keepPid) {
		t.Errorf("child %d of a healthy wrapper died early", keepPid)
	}
}
