package core

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"
)

// The child writes to both streams; under `set -e` a failed write is exit 1,
// as the backup scripts were on 2026-11-13.
const stdioChild = "set -e; echo child-out; echo child-err >&2"

// breakStdio leaves os.Stdout nil and os.Stderr closed, as a long-running conchd
// was found, with the streams conch started with redirected into a file, which
// it returns.
func breakStdio(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "start-stdio")
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()

	oldOut, oldErr, oldStartOut, oldStartErr := os.Stdout, os.Stderr, startStdout, startStderr
	os.Stdout, os.Stderr, startStdout, startStderr = nil, closed, f, f
	t.Cleanup(func() {
		os.Stdout, os.Stderr, startStdout, startStderr = oldOut, oldErr, oldStartOut, oldStartErr
		_ = f.Close()
	})
	return out
}

func assertChildWrote(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "child-out") || !strings.Contains(string(b), "child-err") {
		t.Errorf("child output did not reach the streams conch started with; got %q", b)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestHookGetsStartStdioWhenCurrentIsUnusable(t *testing.T) {
	out := breakStdio(t)
	if err := runHook(context.Background(), discardLogger(), stdioChild, "on-acquire", "stdio", 1, 1, 5*time.Second); err != nil {
		t.Fatalf("hook failed: %v", err)
	}
	assertChildWrote(t, out)
}

func TestFenceGetsStartStdioWhenCurrentIsUnusable(t *testing.T) {
	out := breakStdio(t)
	if err := RunFence(discardLogger(), FenceSpec{Cmd: stdioChild, Budget: 5 * time.Second, Name: "stdio"}, "test"); err != nil {
		t.Fatalf("fence failed: %v", err)
	}
	assertChildWrote(t, out)
}

// The cron path: Run, as conchd runs a job.
func TestRunChildGetsStartStdioWhenCurrentIsUnusable(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := discardLogger()
	sess, err := NewCoreSession(ctx, []string{etcd.ClientURL}, time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()
	holder := &DummyHolder{sess: sess, name: "stdio", key: "/conch/v1/cron/fire/stdio/"}

	out := breakStdio(t)
	code, _, err := Run(ctx, logger, sess, holder, []string{"/bin/sh", "-c", stdioChild}, time.Second)
	if code != 0 || err != nil {
		t.Fatalf("child exit %d, err %v; want 0", code, err)
	}
	assertChildWrote(t, out)
}
