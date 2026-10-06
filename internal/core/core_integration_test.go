package core

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type DummyHolder struct {
	sess *CoreSession
	name string
	key  string
}

func (dh *DummyHolder) Acquire(ctx context.Context) (int64, error) {
	_, err := dh.sess.Client.Put(ctx, dh.key, "dummy", clientv3.WithLease(dh.sess.LeaseID))
	if err != nil {
		return 0, err
	}
	return 12345, nil // dummy revision
}

func (dh *DummyHolder) Release(ctx context.Context) error {
	_, err := dh.sess.Client.Delete(ctx, dh.key)
	return err
}

func (dh *DummyHolder) Name() string {
	return dh.name
}

func (dh *DummyHolder) Key() string {
	return dh.key
}

func TestCoreLeaseLossSupervision(t *testing.T) {
	// Start local etcd
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess, err := NewCoreSession(ctx, []string{etcd.ClientURL}, 1*time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()

	holder := &DummyHolder{
		sess: sess,
		name: "test-lock",
		key:  "/conch/v1/elect/test-lock",
	}

	// We'll run a child that sleeps for a long time
	// We'll revoke the lease and check that the child gets killed and Run returns 70
	killAfter := 2 * time.Second

	// Start core.Run in background
	errCh := make(chan struct {
		code int
		err  error
	}, 1)

	// We run a shell command that starts a background process (grandchild) and sleeps
	// We'll write the grandchild PID to a temp file
	tempFile, err := os.CreateTemp("", "conch-grandchild-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Child script: starts sleep 100 in background (grandchild), writes its PID to tempFile, then sleeps
	childCmd := []string{
		"sh", "-c",
		fmt.Sprintf("sleep 100 & echo $! > %s; wait", tempFile.Name()),
	}

	go func() {
		code, _, err := Run(ctx, logger, sess, holder, childCmd, killAfter)
		errCh <- struct {
			code int
			err  error
		}{code, err}
	}()

	// Wait for child to write grandchild PID
	var grandchildPid int
	for i := 0; i < 50; i++ {
		content, err := os.ReadFile(tempFile.Name())
		if err == nil && len(strings.TrimSpace(string(content))) > 0 {
			pidStr := strings.TrimSpace(string(content))
			if pid, err := strconv.Atoi(pidStr); err == nil {
				grandchildPid = pid
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if grandchildPid == 0 {
		t.Fatalf("failed to get grandchild PID")
	}

	// Verify grandchild is running
	if err := syscall.Kill(grandchildPid, 0); err != nil {
		t.Fatalf("grandchild is not running: %v", err)
	}

	// Revoke lease to trigger loss
	_, err = sess.Client.Revoke(ctx, sess.LeaseID)
	if err != nil {
		t.Fatalf("failed to revoke lease: %v", err)
	}

	// Wait for Run to finish
	select {
	case result := <-errCh:
		if result.code != 70 {
			t.Errorf("expected exit code 70 on lease loss, got %d (err: %v)", result.code, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not finish within timeout after lease revocation")
	}

	// Verify grandchild was also killed (process group killed)
	// We wait up to 2 seconds for it to exit
	killed := false
	for i := 0; i < 20; i++ {
		err := syscall.Kill(grandchildPid, 0)
		if err != nil {
			if err == syscall.ESRCH {
				killed = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !killed {
		t.Errorf("grandchild process %d is still running after process group killed", grandchildPid)
		// Clean it up just in case
		_ = syscall.Kill(grandchildPid, syscall.SIGKILL)
	}
}

type runResult struct {
	code    int
	outcome Outcome
	err     error
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startHookRun starts etcd and a session (TTL 6s) holding a DummyHolder, then
// runs RunWithConfig in the background. The returned buffer collects the log.
func startHookRun(t *testing.T, cfg RunConfig, child []string) (*CoreSession, chan runResult, *syncBuffer) {
	t.Helper()
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	t.Cleanup(etcd.Stop)

	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(os.Stderr, logs), &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sess, err := NewCoreSession(ctx, []string{etcd.ClientURL}, time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	t.Cleanup(sess.Close)

	holder := &DummyHolder{sess: sess, name: "hook-lock", key: "/conch/v1/elect/hook-lock"}
	res := make(chan runResult, 1)
	go func() {
		code, outcome, err := RunWithConfig(ctx, logger, sess, holder, child, 2*time.Second, cfg)
		res <- runResult{code, outcome, err}
	}()
	return sess, res, logs
}

func waitForFile(path string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestOnAcquireHookKilledOnLeaseLoss (H2): losing the lease while --on-acquire
// runs must kill the hook promptly, not after it finishes, and the child must
// never start.
func TestOnAcquireHookKilledOnLeaseLoss(t *testing.T) {
	dir := t.TempDir()
	hookPid := dir + "/hook.pid"
	markChild := dir + "/child"

	sess, res, logs := startHookRun(t, RunConfig{
		OnAcquire:   "echo $$ > " + hookPid + "; exec sleep 20",
		HookTimeout: 30 * time.Second,
	}, []string{"touch", markChild})

	if !waitForFile(hookPid, 5*time.Second) {
		t.Fatalf("on-acquire hook never started")
	}
	time.Sleep(time.Second)
	revoked := time.Now()
	if _, err := sess.Client.Revoke(context.Background(), sess.LeaseID); err != nil {
		t.Fatalf("failed to revoke lease: %v", err)
	}

	// One keepalive detection window at TTL 6s is 2s + 1.5s.
	var r runResult
	select {
	case r = <-res:
	case <-time.After(10 * time.Second):
		t.Fatalf("RunWithConfig still running 10s after lease revocation")
	}
	elapsed := time.Since(revoked)
	t.Logf("returned %v after revocation: code=%d outcome=%s err=%v", elapsed, r.code, r.outcome, r.err)

	if elapsed > 4*time.Second {
		t.Errorf("hook was not cut short: returned %v after lease revocation", elapsed)
	}
	if b, err := os.ReadFile(hookPid); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
			if syscall.Kill(pid, 0) == nil {
				t.Errorf("hook process %d still alive after RunWithConfig returned", pid)
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
	// The child may be signalled before it gets to run, so check the log too.
	if _, err := os.Stat(markChild); err == nil || strings.Contains(logs.String(), "msg=child-start") {
		t.Errorf("child started although the lease was lost during on-acquire")
	}
	if r.code != 70 || r.outcome != OutcomeHoldLost {
		t.Errorf("expected exit 70 / %s, got %d / %s", OutcomeHoldLost, r.code, r.outcome)
	}
}

// TestOnLoseRunsWhenLeaseLostDuringOnAcquire (H2): once --on-acquire has
// started (e.g. promoted a database), --on-lose must run on loss even though
// the child never started, and the child must not start.
func TestOnLoseRunsWhenLeaseLostDuringOnAcquire(t *testing.T) {
	dir := t.TempDir()
	markHook := dir + "/hook"
	markLose := dir + "/lose"
	markChild := dir + "/child"

	sess, res, logs := startHookRun(t, RunConfig{
		OnAcquire:   "touch " + markHook + "; sleep 2",
		OnLose:      "touch " + markLose,
		HookTimeout: 30 * time.Second,
	}, []string{"touch", markChild})

	if !waitForFile(markHook, 5*time.Second) {
		t.Fatalf("on-acquire hook never started")
	}
	if _, err := sess.Client.Revoke(context.Background(), sess.LeaseID); err != nil {
		t.Fatalf("failed to revoke lease: %v", err)
	}

	select {
	case r := <-res:
		t.Logf("code=%d outcome=%s err=%v", r.code, r.outcome, r.err)
	case <-time.After(15 * time.Second):
		t.Fatalf("RunWithConfig did not return")
	}
	// The child may be signalled before it gets to run, so check the log too.
	if _, err := os.Stat(markChild); err == nil || strings.Contains(logs.String(), "msg=child-start") {
		t.Errorf("child started although the lease was lost during on-acquire")
	}
	if _, err := os.Stat(markLose); err != nil {
		t.Errorf("on-lose did not run after on-acquire started and the lease was lost")
	}
}
