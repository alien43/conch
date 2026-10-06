//go:build linux

package core

import (
	"context"
	"fmt"
	"log/slog"
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

// TestSetsidGrandchildEscapesLeaseLoss (H4) pins a known limitation: a
// descendant that leaves the child's process group (setsid, daemonizing)
// is not reached by the group kill on lease loss. Run conch as the main
// process of a systemd unit (KillMode=control-group) to catch these.
func TestSetsidGrandchildEscapesLeaseLoss(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess, err := NewCoreSession(ctx, []string{etcd.ClientURL}, time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()
	holder := &DummyHolder{sess: sess, name: "h4", key: "/conch/v1/elect/h4"}

	pidfile := filepath.Join(t.TempDir(), "grandchild.pid")
	child := []string{"sh", "-c", fmt.Sprintf("setsid sleep 300 & echo $! > %s; sleep 300", pidfile)}

	res := make(chan int, 1)
	go func() {
		code, _, _ := Run(ctx, logger, sess, holder, child, 2*time.Second)
		res <- code
	}()

	gc := readPid(t, pidfile)
	defer func() { _ = syscall.Kill(gc, syscall.SIGKILL) }()

	if _, err := sess.Client.Revoke(ctx, sess.LeaseID); err != nil {
		t.Fatalf("failed to revoke lease: %v", err)
	}
	select {
	case code := <-res:
		if code != 70 {
			t.Errorf("expected exit 70 on lease loss, got %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after lease loss")
	}

	time.Sleep(time.Second)
	if !alive(gc) {
		t.Errorf("setsid grandchild %d died: the limitation documented for H4 no longer holds", gc)
	}
}
