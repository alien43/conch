//go:build linux

package elect

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"
	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func buildConch(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		if filepath.Dir(dir) == dir {
			t.Fatal("go.mod not found")
		}
		dir = filepath.Dir(dir)
	}
	conchPath := filepath.Join(t.TempDir(), "conch")
	out, err := exec.Command("go", "build", "-o", conchPath, filepath.Join(dir, "cmd", "conch")).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build conch: %v\n%s", err, out)
	}
	return conchPath
}

// measureOverlap partitions a leader whose child ignores SIGTERM and returns
// how long that child kept running after a rival's child started. Negative
// means the leader's child was dead that long before the rival started.
// extraArgs go to the partitioned leader's command line.
func measureOverlap(t *testing.T, conch string, ttl time.Duration, extraArgs ...string) time.Duration {
	t.Helper()
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()
	proxy, err := testutil.StartProxy(etcd.ClientURL)
	if err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}
	defer proxy.Close()

	dir := t.TempDir()
	pidA := filepath.Join(dir, "a.pid")
	startB := filepath.Join(dir, "b.start")
	office := "h5"

	argsA := append([]string{"elect", office, "--endpoints", proxy.Addr(), "--ttl", ttl.String()}, extraArgs...)
	argsA = append(argsA, "--", "sh", "-c", "trap '' TERM; echo $$ > "+pidA+"; while :; do sleep 0.05; done")
	a := exec.Command(conch, argsA...)
	a.Stderr = os.Stderr
	if err := a.Start(); err != nil {
		t.Fatalf("failed to start leader: %v", err)
	}
	defer func() { _ = a.Process.Kill(); _ = a.Wait() }()
	childA := waitPid(t, pidA)
	defer func() { _ = syscall.Kill(childA, syscall.SIGKILL) }()

	b := exec.Command(conch, "elect", office, "--endpoints", etcd.ClientURL, "--ttl", ttl.String(),
		"--", "sh", "-c", "date +%s%N > "+startB+"; exec sleep 300")
	b.Stderr = os.Stderr
	if err := b.Start(); err != nil {
		t.Fatalf("failed to start rival: %v", err)
	}
	defer func() { _ = b.Process.Kill(); _ = b.Wait() }()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcd.ClientURL}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	for i := 0; ; i++ {
		resp, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err == nil && resp.Count == 2 {
			break
		}
		if i > 100 {
			t.Fatalf("rival never queued")
		}
		time.Sleep(50 * time.Millisecond)
	}

	proxy.Blackhole(true)
	partitioned := time.Now()

	var endA time.Time
	deadline := partitioned.Add(2*ttl + 15*time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(childA, 0) != nil {
			endA = time.Now()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if endA.IsZero() {
		t.Fatalf("partitioned leader's child never died")
	}

	var bStart time.Time
	for time.Now().Before(deadline) {
		if s, err := os.ReadFile(startB); err == nil {
			if ns, err := strconv.ParseInt(strings.TrimSpace(string(s)), 10, 64); err == nil {
				bStart = time.Unix(0, ns)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if bStart.IsZero() {
		t.Fatalf("rival never started")
	}
	t.Logf("ttl=%v args=%v: leader child died %v after partition, rival started %v after partition, overlap %v",
		ttl, extraArgs, endA.Sub(partitioned).Round(time.Millisecond), bStart.Sub(partitioned).Round(time.Millisecond), endA.Sub(bStart).Round(time.Millisecond))
	return endA.Sub(bStart)
}

func waitPid(t *testing.T, path string) int {
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

// TestElectDefaultKillAfterBeatsRival (H5): with default flags, a partitioned
// leader whose child ignores SIGTERM must have its child dead at least 1s
// before a rival can start.
func TestElectDefaultKillAfterBeatsRival(t *testing.T) {
	overlap := measureOverlap(t, buildConch(t), 10*time.Second)
	if overlap > -time.Second {
		t.Errorf("leader's child died only %v before the rival started (positive = overlap); want at least 1s of slack", -overlap)
	}
}

// TestElectMeasureOverlap records the overlap at a few settings for the
// record. Slow (two 30s-TTL runs), so only with CONCH_MEASURE_OVERLAP=1.
func TestElectMeasureOverlap(t *testing.T) {
	if os.Getenv("CONCH_MEASURE_OVERLAP") == "" {
		t.Skip("set CONCH_MEASURE_OVERLAP=1 to run")
	}
	conch := buildConch(t)
	for _, c := range []struct {
		ttl  time.Duration
		args []string
	}{
		{10 * time.Second, []string{"--kill-after", "5s"}},
		{30 * time.Second, []string{"--kill-after", "5s"}},
		{10 * time.Second, nil},
		{30 * time.Second, nil},
	} {
		measureOverlap(t, conch, c.ttl, c.args...)
	}
}
