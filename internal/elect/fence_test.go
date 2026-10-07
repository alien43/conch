package elect

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"
	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// fakeFence writes a fence script that appends "<start> <end> <reason> <rev>"
// to log on every run and then exits with what body leaves in $rc.
func fakeFence(t *testing.T, dir, body string) (script, log string) {
	t.Helper()
	log = filepath.Join(dir, "fence.log")
	script = filepath.Join(dir, "fence.sh")
	src := fmt.Sprintf(`#!/bin/sh
start=$(date +%%s.%%N)
rc=0
%s
echo "$start $(date +%%s.%%N) $CONCH_FENCE_REASON $CONCH_REV" >> %q
exit $rc
`, body, log)
	if err := os.WriteFile(script, []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}
	return script, log
}

type fenceRun struct {
	start, end time.Time
	reason     string
	rev        string
}

func readFenceLog(t *testing.T, log string) []fenceRun {
	t.Helper()
	b, err := os.ReadFile(log)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var runs []fenceRun
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		r := fenceRun{start: unixFloat(t, f[0]), end: unixFloat(t, f[1]), reason: f[2]}
		if len(f) > 3 {
			r.rev = f[3]
		}
		runs = append(runs, r)
	}
	return runs
}

func unixFloat(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("bad timestamp %q: %v", s, err)
	}
	return time.Unix(0, int64(v*1e9))
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// waitKey waits for the office to have a holder and returns its key.
func waitKey(t *testing.T, cli *clientv3.Client, office string) string {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		r, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix())
		if err == nil && len(r.Kvs) > 0 {
			return string(r.Kvs[0].Key)
		}
	}
	t.Fatalf("office %s never got a holder", office)
	return ""
}

// waitGone returns when key disappears.
func waitGone(t *testing.T, cli *clientv3.Client, key string, within time.Duration) time.Time {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		r, err := cli.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Kvs) == 0 {
			return time.Now()
		}
	}
	t.Fatalf("key %s still present after %v", key, within)
	return time.Time{}
}

func fenceEtcd(t *testing.T) (*testutil.TestEtcd, *clientv3.Client) {
	t.Helper()
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	t.Cleanup(etcd.Stop)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcd.ClientURL}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	return etcd, cli
}

// TestFenceOnLeaseLoss: cut conch off from etcd. The fence must start, and
// confirm, while the lease still exists server-side, with at least the budget
// to spare.
func TestFenceOnLeaseLoss(t *testing.T) {
	testFenceOnLeaseLoss(t, false)
}

// TestFenceWedgedChild: the same, with the child SIGSTOPped first. The fence's
// timing must not depend on the child.
func TestFenceWedgedChild(t *testing.T) {
	testFenceOnLeaseLoss(t, true)
}

func testFenceOnLeaseLoss(t *testing.T, wedge bool) {
	etcd, observer := fenceEtcd(t)
	px, err := testutil.StartProxy(strings.TrimPrefix(etcd.ClientURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer px.Close()

	dir := t.TempDir()
	script, log := fakeFence(t, dir, "")
	pidfile := filepath.Join(dir, "child.pid")
	ttl, budget, killAfter := 6*time.Second, time.Second, 2*time.Second
	if why := core.FitFence(ttl, budget); why != "" {
		t.Fatal(why)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codeCh := make(chan int, 1)
	go func() {
		code, _ := RunElectFenced(ctx, quietLogger(), []string{px.Addr()}, time.Second, ttl, killAfter, false, "fenced", 0, 60*time.Second,
			"", "", time.Second, Fence{Cmd: script, Budget: budget},
			[]string{"/bin/sh", "-c", "echo $$ > " + pidfile + "; exec sleep 1000"})
		codeCh <- code
	}()

	key := waitKey(t, observer, "fenced")
	time.Sleep(2500 * time.Millisecond) // a couple of renewals
	if wedge {
		pid := waitPid(t, pidfile)
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			t.Fatal(err)
		}
	}
	px.Blackhole(true)
	gone := waitGone(t, observer, key, 3*ttl)

	select {
	case code := <-codeCh:
		if code != 70 {
			t.Fatalf("exit %d, want 70", code)
		}
	case <-time.After(killAfter + budget + 5*time.Second):
		t.Fatal("conch did not exit after losing the lease")
	}
	runs := readFenceLog(t, log)
	if len(runs) != 1 {
		t.Fatalf("fence ran %d times, want 1: %+v", len(runs), runs)
	}
	r := runs[0]
	if r.reason != core.FenceReasonLost && r.reason != core.FenceReasonLeaseDeadline {
		t.Fatalf("fence reason %q", r.reason)
	}
	slack := gone.Sub(r.start)
	t.Logf("fence started at %s, confirmed %s later, key vanished %s after start (slack %s, budget %s, reason %s)",
		r.start.Format("15:04:05.000"), r.end.Sub(r.start).Round(time.Millisecond), slack.Round(time.Millisecond), slack.Round(time.Millisecond), budget, r.reason)
	if slack <= budget {
		t.Fatalf("fence started only %s before the lease expired; budget %s", slack, budget)
	}
	if !r.end.Before(gone) {
		t.Fatalf("fence confirmed at %s, after the lease expired at %s", r.end, gone)
	}
}

// TestFenceRunsBeforeResign: a child that exits normally. The fence must have
// confirmed before the office key is deleted, so no rival can win first.
func TestFenceRunsBeforeResign(t *testing.T) {
	etcd, observer := fenceEtcd(t)
	dir := t.TempDir()
	script, log := fakeFence(t, dir, "sleep 0.5")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	codeCh := make(chan int, 1)
	go func() {
		code, _ := RunElectFenced(ctx, quietLogger(), []string{etcd.ClientURL}, time.Second, 6*time.Second, 2*time.Second, false, "resign", 0, 60*time.Second,
			"", "", time.Second, Fence{Cmd: script, Budget: time.Second}, []string{"/bin/sh", "-c", "sleep 1; exit 3"})
		codeCh <- code
	}()
	key := waitKey(t, observer, "resign")
	gone := waitGone(t, observer, key, 10*time.Second)
	if code := <-codeCh; code != 3 {
		t.Fatalf("exit %d, want the child's 3", code)
	}
	runs := readFenceLog(t, log)
	if len(runs) != 1 || runs[0].reason != core.FenceReasonChildExit {
		t.Fatalf("fence runs %+v, want one child-exit", runs)
	}
	if runs[0].rev == "" || runs[0].rev == "0" {
		t.Fatalf("CONCH_REV not passed to the fence: %+v", runs[0])
	}
	if !runs[0].end.Before(gone) {
		t.Fatalf("office resigned at %s, before the fence confirmed at %s", gone, runs[0].end)
	}
	t.Logf("fence confirmed %s before resign", gone.Sub(runs[0].end).Round(time.Millisecond))
}

// TestFenceBudgetExceeded: a fence that hangs is killed at its budget and the
// run fails with 71; the office is not resigned, so its key outlives conch
// until the lease expires.
func TestFenceBudgetExceeded(t *testing.T) {
	etcd, observer := fenceEtcd(t)
	dir := t.TempDir()
	script, _ := fakeFence(t, dir, "sleep 100")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	codeCh := make(chan int, 1)
	go func() {
		code, _ := RunElectFenced(ctx, quietLogger(), []string{etcd.ClientURL}, time.Second, 6*time.Second, 2*time.Second, false, "hang", 0, 60*time.Second,
			"", "", time.Second, Fence{Cmd: script, Budget: time.Second}, []string{"true"})
		codeCh <- code
	}()
	key := waitKey(t, observer, "hang")
	var code int
	select {
	case code = <-codeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("conch did not give up on a hung fence at its budget")
	}
	exited := time.Now()
	if code != core.ExitFenceFailed {
		t.Fatalf("exit %d, want %d", code, core.ExitFenceFailed)
	}
	if took := exited.Sub(start); took > 4*time.Second {
		t.Fatalf("took %s with a 1s budget", took)
	}
	r, err := observer.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Kvs) == 0 {
		t.Fatal("office key deleted right after a failed fence: it was resigned or revoked")
	}
	gone := waitGone(t, observer, key, 10*time.Second)
	t.Logf("office key outlived conch by %s (lease expiry, not resign)", gone.Sub(exited).Round(time.Millisecond))
}

// TestFenceFailedBlocksRecampaign: under --restart, a fence that fails twice
// is retried, and the child is not started again until a retry confirmed.
func TestFenceFailedBlocksRecampaign(t *testing.T) {
	etcd, _ := fenceEtcd(t)
	dir := t.TempDir()
	count := filepath.Join(dir, "count")
	script, log := fakeFence(t, dir, fmt.Sprintf(`n=$(cat %q 2>/dev/null || echo 0); n=$((n+1)); echo $n > %q; [ $n -le 2 ] && rc=1`, count, count))
	starts := filepath.Join(dir, "starts")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = RunElectFenced(ctx, quietLogger(), []string{etcd.ClientURL}, time.Second, 6*time.Second, 2*time.Second, true, "retry", 0, 60*time.Second,
			"", "", time.Second, Fence{Cmd: script, Budget: time.Second},
			[]string{"/bin/sh", "-c", "date +%s.%N >> " + starts + "; sleep 0.3"})
	}()

	var childStarts []time.Time
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		b, _ := os.ReadFile(starts)
		lines := strings.Fields(string(b))
		if len(lines) >= 2 {
			for _, l := range lines {
				childStarts = append(childStarts, unixFloat(t, l))
			}
			break
		}
	}
	cancel()
	if len(childStarts) < 2 {
		t.Fatalf("child started %d times in 30s, want 2", len(childStarts))
	}
	runs := readFenceLog(t, log)
	if len(runs) < 3 {
		t.Fatalf("fence ran %d times before the second start, want ≥3: %+v", len(runs), runs)
	}
	if runs[0].reason != core.FenceReasonChildExit || runs[1].reason != core.FenceReasonRetry || runs[2].reason != core.FenceReasonRetry {
		t.Fatalf("fence reasons %q %q %q, want child-exit retry retry", runs[0].reason, runs[1].reason, runs[2].reason)
	}
	if !childStarts[1].After(runs[2].end) {
		t.Fatalf("child restarted at %s, before the first confirmed fence at %s", childStarts[1], runs[2].end)
	}
	t.Logf("fences: fail %s, fail %s, ok %s; second child start %s",
		runs[0].start.Format("15:04:05.000"), runs[1].start.Format("15:04:05.000"), runs[2].start.Format("15:04:05.000"), childStarts[1].Format("15:04:05.000"))
}

func TestFitFence(t *testing.T) {
	for _, c := range []struct {
		ttl, budget time.Duration
		ok          bool
	}{
		{10 * time.Second, 3 * time.Second, true},          // 5 + 3 + 1 < 10
		{10 * time.Second, 4100 * time.Millisecond, false}, // 5 + 4.1 + 1 > 10
		{6 * time.Second, time.Second, true},               // 3.5 + 1 + 1 < 6
		{30 * time.Second, 13 * time.Second, true},         // 15 + 13 + 1 < 30
		{10 * time.Second, 0, false},
	} {
		if got := core.FitFence(c.ttl, c.budget) == ""; got != c.ok {
			t.Errorf("FitFence(%v, %v) ok=%v, want %v", c.ttl, c.budget, got, c.ok)
		}
	}
}

// petLog is a fake NOTIFY_SOCKET recording when each WATCHDOG=1 arrived.
type petLog struct {
	mu   sync.Mutex
	pets []time.Time
}

func listenNotify(t *testing.T) (string, *petLog) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "notify.sock")
	c, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	l := &petLog{}
	go func() {
		buf := make([]byte, 64)
		for {
			n, _, err := c.ReadFromUnix(buf)
			if err != nil {
				return
			}
			if string(buf[:n]) == "WATCHDOG=1" {
				l.mu.Lock()
				l.pets = append(l.pets, time.Now())
				l.mu.Unlock()
			}
		}
	}()
	return path, l
}

func (l *petLog) between(a, b time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, p := range l.pets {
		if p.After(a) && p.Before(b) {
			n++
		}
	}
	return n
}

// TestWatchdogAcrossAFailedFence: conch pets while it holds the office, stops
// petting from a failed fence until a retry confirms, then pets again.
func TestWatchdogAcrossAFailedFence(t *testing.T) {
	etcd, _ := fenceEtcd(t)
	dir := t.TempDir()
	count := filepath.Join(dir, "count")
	script, log := fakeFence(t, dir, fmt.Sprintf(`n=$(cat %q 2>/dev/null || echo 0); n=$((n+1)); echo $n > %q; [ $n -le 2 ] && rc=1`, count, count))
	sock, pets := listenNotify(t)
	wd := core.NewWatchdog(sock, 50*time.Millisecond, "", 0, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go func() {
		_, _ = RunElectFenced(ctx, quietLogger(), []string{etcd.ClientURL}, time.Second, 6*time.Second, 2*time.Second, true, "wd", 0, 60*time.Second,
			"", "", time.Second, Fence{Cmd: script, Budget: time.Second, Watchdog: wd}, []string{"sleep", "1"})
	}()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline) && len(readFenceLog(t, log)) < 3; time.Sleep(50 * time.Millisecond) {
	}
	time.Sleep(500 * time.Millisecond)
	cancel()
	runs := readFenceLog(t, log)
	if len(runs) < 3 {
		t.Fatalf("fence ran %d times, want 3", len(runs))
	}
	holding := pets.between(start, runs[0].start)
	starved := pets.between(runs[0].end.Add(100*time.Millisecond), runs[2].start)
	after := pets.between(runs[2].end.Add(100*time.Millisecond), runs[2].end.Add(500*time.Millisecond))
	t.Logf("pets: %d while holding, %d between the failed fence and the confirming retry (%s), %d in the 400ms after it",
		holding, starved, runs[2].start.Sub(runs[0].end).Round(time.Millisecond), after)
	if holding < 5 {
		t.Fatalf("only %d pets while holding", holding)
	}
	if starved != 0 {
		t.Fatalf("%d pets while a fence was failed", starved)
	}
	if after < 3 {
		t.Fatalf("only %d pets after the confirming retry", after)
	}
}
