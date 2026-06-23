package elect

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestElectWhoVacant(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcd.ClientURL},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()
	code, err := CmdWho(ctx, cli, "nonexistent-office", false)
	if err != nil {
		t.Fatalf("CmdWho returned error: %v", err)
	}
	if code != 1 {
		t.Errorf("expected exit code 1 for vacant office, got %d", code)
	}
}

func TestElectHandoverImmediately(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Candidate A runs a quick command and exits cleanly
	tStart := time.Now()
	codeA, err := RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-1", 0, 60*time.Second, "", "", 1*time.Second, []string{"true"})
	if err != nil {
		t.Fatalf("Candidate A failed: %v", err)
	}
	if codeA != 0 {
		t.Fatalf("Candidate A exited with code %d", codeA)
	}

	// 2. Candidate B should be able to acquire immediately (less than 1s)
	tAcquireStart := time.Now()
	codeB, err := RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-1", 0, 60*time.Second, "", "", 1*time.Second, []string{"true"})
	if err != nil {
		t.Fatalf("Candidate B failed: %v", err)
	}
	if codeB != 0 {
		t.Fatalf("Candidate B exited with code %d", codeB)
	}

	dur := time.Since(tAcquireStart)
	if dur >= 2*time.Second {
		t.Errorf("expected immediate handover (< 2s), but took %v (indicates we might have waited for TTL)", dur)
	}
	t.Logf("Candidate B won after %v (handover succeeded immediately!)", dur)
	_ = tStart
}

func TestElectWinnerRevEnv(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We'll run a shell command that outputs CONCH_REV and CONCH_LEASE to a temp file
	tempFile, err := os.CreateTemp("", "conch-elect-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	childCmd := []string{
		"sh", "-c",
		"echo $CONCH_REV $CONCH_LEASE > " + tempFile.Name(),
	}

	code, err := RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-rev", 0, 60*time.Second, "", "", 1*time.Second, childCmd)
	if err != nil {
		t.Fatalf("RunElect failed: %v", err)
	}
	if code != 0 {
		t.Fatalf("Child exited with code %d", code)
	}

	content, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read temp file: %v", err)
	}

	parts := strings.Fields(string(content))
	if len(parts) < 2 {
		t.Fatalf("expected CONCH_REV and CONCH_LEASE in file, got: %s", string(content))
	}

	rev, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || rev <= 0 {
		t.Errorf("invalid CONCH_REV: %s", parts[0])
	}

	leaseHex := parts[1]
	if leaseHex == "" {
		t.Errorf("empty CONCH_LEASE")
	}

	t.Logf("verified env: CONCH_REV=%d, CONCH_LEASE=%s", rev, leaseHex)
}

func TestElectRestartBackoffReset(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateFile, err := os.CreateTemp("", "conch-backoff-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	stateFile.Close()
	defer os.Remove(stateFile.Name())

	// Write "0" to state file (meaning first run)
	_ = os.WriteFile(stateFile.Name(), []byte("0"), 0644)

	logFile, err := os.CreateTemp("", "conch-backoff-log")
	if err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}
	logFile.Close()
	defer os.Remove(logFile.Name())

	// The child script:
	// - Appends current time in UnixNano to logFile
	// - Reads stateFile
	// - If state is "0": writes "1" to stateFile and exits immediately (exit 1)
	// - If state is "1": writes "2" to stateFile and sleeps for 1.5s (survives stableChildThreshold), then exits (exit 1)
	// - If state is "2": writes "3" to stateFile and exits immediately (exit 1)
	childScript := fmt.Sprintf(`
		date +%%s%%N >> %s
		val=$(cat %s)
		if [ "$val" = "0" ]; then
			echo "1" > %s
			exit 1
		elif [ "$val" = "1" ]; then
			echo "2" > %s
			sleep 1.5
			exit 1
		else
			echo "3" > %s
			exit 1
		fi
	`, logFile.Name(), stateFile.Name(), stateFile.Name(), stateFile.Name(), stateFile.Name())

	done := make(chan struct{})
	go func() {
		_, _ = RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, true, "office-backoff", 0, 1*time.Second, "", "", 1*time.Second, []string{"sh", "-c", childScript})
		close(done)
	}()

	// Wait long enough for 3 runs to execute
	time.Sleep(5 * time.Second)
	cancel() // Stop the campaign
	<-done   // Wait for RunElect to finish and close

	// Read logFile
	content, err := os.ReadFile(logFile.Name())
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}

	lines := strings.Fields(string(content))
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 runs, got %d. Log content: %s", len(lines), string(content))
	}

	t1, _ := strconv.ParseInt(lines[0], 10, 64)
	t2, _ := strconv.ParseInt(lines[1], 10, 64)
	t3, _ := strconv.ParseInt(lines[2], 10, 64)

	time1 := time.Unix(0, t1)
	time2 := time.Unix(0, t2)
	time3 := time.Unix(0, t3)

	dur1 := time2.Sub(time1)
	t.Logf("Run 1 to Run 2 interval: %v", dur1)

	run2End := time2.Add(1500 * time.Millisecond)
	dur2 := time3.Sub(run2End)
	t.Logf("Run 2 completion to Run 3 start interval: %v", dur2)

	if dur2 >= 1800*time.Millisecond {
		t.Errorf("backoff was NOT reset: expected sleep duration around 1s, but got %v", dur2)
	} else {
		t.Logf("success: backoff was successfully reset (sleep dur %v < 1.8s)", dur2)
	}
}

func TestElectAssert(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// 1. Vacant office assertion
	code, err := CmdAssert(ctx, cli, "vacant-office", 0, false)
	if err != nil {
		t.Fatalf("CmdAssert returned error: %v", err)
	}
	if code != 1 {
		t.Errorf("expected exit code 1 for vacant office, got %d", code)
	}

	// 2. Win the office and assert it
	done := make(chan struct{})
	go func() {
		// Run candidate that sleeps for 5 seconds (keeps holding the office)
		_, _ = RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-assert", 0, 60*time.Second, "", "", 1*time.Second, []string{"sleep", "5"})
		close(done)
	}()

	// Wait for the candidate to win and hold the office
	time.Sleep(1 * time.Second)

	// Now assert
	code, err = CmdAssert(ctx, cli, "office-assert", 0, false)
	if err != nil {
		t.Fatalf("CmdAssert failed: %v", err)
	}
	if code != 0 {
		t.Errorf("expected exit code 0 for held office, got %d", code)
	}

	// Assert with valid min-rev (should pass)
	code, err = CmdAssert(ctx, cli, "office-assert", 1, false)
	if err != nil {
		t.Fatalf("CmdAssert with minRev=1 failed: %v", err)
	}
	if code != 0 {
		t.Errorf("expected exit code 0 for minRev=1, got %d", code)
	}

	// Assert with too high min-rev (should fail with exit code 1)
	code, err = CmdAssert(ctx, cli, "office-assert", 1000000, false)
	if err != nil {
		t.Fatalf("CmdAssert with minRev=1000000 failed: %v", err)
	}
	if code != 1 {
		t.Errorf("expected exit code 1 for too high min-rev, got %d", code)
	}

	// 3. Mock office with another host name
	mockKey := "/conch/v1/elect/other-office/candidate-key"
	mockVal := `{"host":"some-other-host-name","pid":12345,"started":"2026-06-14T00:00:00Z"}`
	_, err = cli.Put(ctx, mockKey, mockVal)
	if err != nil {
		t.Fatalf("failed to write mock leader: %v", err)
	}

	code, err = CmdAssert(ctx, cli, "other-office", 0, false)
	if err != nil {
		t.Fatalf("CmdAssert for other-office failed: %v", err)
	}
	if code != 1 {
		t.Errorf("expected exit code 1 for different host holding office, got %d", code)
	}

	cancel()
	<-done
}

func TestElectHooks(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempFile, err := os.CreateTemp("", "conch-hooks-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempFile.Close()
	defer os.Remove(tempFile.Name())

	// Use hooks to write to temp file
	onAcquire := fmt.Sprintf("echo ACQUIRE $CONCH_NAME >> %s", tempFile.Name())
	onLose := fmt.Sprintf("echo LOSE $CONCH_NAME >> %s", tempFile.Name())

	code, err := RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-hooks", 0, 60*time.Second, onAcquire, onLose, 2*time.Second, []string{"sh", "-c", "echo CHILD >> " + tempFile.Name()})
	if err != nil {
		t.Fatalf("RunElect failed: %v", err)
	}
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}

	content, err := os.ReadFile(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to read temp file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected exactly 3 lines in log, got %d. Content:\n%s", len(lines), string(content))
	}

	if lines[0] != "ACQUIRE office-hooks" {
		t.Errorf("expected first line to be ACQUIRE office-hooks, got %q", lines[0])
	}
	if lines[1] != "CHILD" {
		t.Errorf("expected second line to be CHILD, got %q", lines[1])
	}
	if lines[2] != "LOSE office-hooks" {
		t.Errorf("expected third line to be LOSE office-hooks, got %q", lines[2])
	}

	// Test failing on-acquire hook
	tempFile2, err := os.CreateTemp("", "conch-hooks-fail-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tempFile2.Close()
	defer os.Remove(tempFile2.Name())

	onAcquireFail := "exit 42"
	code, err = RunElect(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, false, "office-hooks-fail", 0, 60*time.Second, onAcquireFail, onLose, 2*time.Second, []string{"sh", "-c", "echo CHILD2 >> " + tempFile2.Name()})
	if code != 75 {
		t.Errorf("expected exit code 75 for failed on-acquire, got %d", code)
	}

	// Verify child did not run
	content2, err := os.ReadFile(tempFile2.Name())
	if err != nil {
		t.Fatalf("failed to read log: %v", err)
	}
	if len(content2) > 0 {
		t.Errorf("child ran even though on-acquire hook failed. Content: %q", string(content2))
	}
}

func TestElectWatch(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcd.ClientURL},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	// Redirect Stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = CmdWatch(ctx, cli, "office-watch", false)
	}()

	// Wait for watch to establish
	time.Sleep(500 * time.Millisecond)

	// Campaign a leader
	mockKey := "/conch/v1/elect/office-watch/candidate-key"
	mockVal := `{"host":"node-watch","pid":12345,"started":"2026-06-14T00:00:00Z"}`
	_, err = cli.Put(context.Background(), mockKey, mockVal)
	if err != nil {
		t.Fatalf("failed to put key: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Cancel context to stop CmdWatch
	cancel()
	<-done

	w.Close()
	os.Stdout = oldStdout
	_, _ = io.Copy(&buf, r)

	output := buf.String()
	if !strings.Contains(output, "office-watch -") {
		t.Errorf("expected initial vacant output, got: %q", output)
	}
	if !strings.Contains(output, "office-watch node-watch pid=12345") {
		t.Errorf("expected leader notification, got: %q", output)
	}
}
