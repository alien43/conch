package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"conch/internal/testutil"

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
