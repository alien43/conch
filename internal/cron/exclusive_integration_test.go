package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"
	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// putExclusiveJob writes the job spec straight to etcd, so these tests also
// compile (and fail, rather than not build) against a conch without the flag.
func putExclusiveJob(t *testing.T, ctx context.Context, cli *clientv3.Client, name, schedule, runTTL, script string) {
	t.Helper()
	spec := map[string]interface{}{
		"schedule":  schedule,
		"cmd":       []string{"sh", "-c", script},
		"run_ttl":   runTTL,
		"exclusive": true,
		"added_by":  "test",
		"added_at":  time.Now().UTC().Format(time.RFC3339),
	}
	b, _ := json.Marshal(spec)
	if _, err := cli.Put(ctx, core.CronJobKey(name), string(b)); err != nil {
		t.Fatalf("failed to put job: %v", err)
	}
}

func startConchds(t *testing.T, ctx context.Context, endpoints []string, n int, logger *slog.Logger) {
	t.Helper()
	for i := 0; i < n; i++ {
		c, err := NewConchd(endpoints, time.Second, 10*time.Second, logger)
		if err != nil {
			t.Fatalf("failed to create conchd %d: %v", i, err)
		}
		go func() { _ = c.Run(ctx) }()
	}
}

// TestCronExclusiveSkipsWhileRunLive is the exclusive counterpart of
// TestCronTicksMayOverlapAcrossNodes: same two nodes, same over-long run, but
// the job holds the per-job lock, so a tick that comes due while the previous
// run is live is skipped (and recorded as skipped) instead of overlapping it.
func TestCronExclusiveSkipsWhileRunLive(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startConchds(t, ctx, endpoints, 2, logger)

	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	logFile := t.TempDir() + "/runs.log"
	script := `s=$(date +%s%N); sleep 5; echo "$CONCH_LEASE $s $(date +%s%N)" >> ` + logFile
	putExclusiveJob(t, ctx, cli, "excl", "@every 2s", "10s", script)

	time.Sleep(14 * time.Second)

	b, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("no runs logged: %v", err)
	}
	type run struct {
		node       string
		start, end int64
	}
	var runs []run
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var r run
		if _, err := fmt.Sscanf(line, "%s %d %d", &r.node, &r.start, &r.end); err == nil {
			runs = append(runs, r)
		}
	}
	for i := range runs {
		for j := i + 1; j < len(runs); j++ {
			if runs[i].start < runs[j].end && runs[j].start < runs[i].end {
				t.Errorf("exclusive runs overlapped: %+v and %+v", runs[i], runs[j])
			}
		}
	}

	resp, err := cli.Get(context.Background(), core.CronResultPrefix("excl"), clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to read results: %v", err)
	}
	skipped := 0
	for _, kv := range resp.Kvs {
		var res map[string]interface{}
		if json.Unmarshal(kv.Value, &res) == nil && res["skipped"] == true {
			skipped++
		}
	}
	if skipped == 0 {
		t.Errorf("no tick recorded as skipped among %d results", len(resp.Kvs))
	}
	t.Logf("%d runs, %d results, %d skipped", len(runs), len(resp.Kvs), skipped)
	cancel()
}

// TestCronExclusiveLockFreedOnSessionLoss: the per-job lock lives on the
// holder's session lease, so when that lease is lost (a crashed or
// partitioned node) the lock goes with it within the session TTL, and another
// node runs the next tick. It must not stay locked until run_ttl.
func TestCronExclusiveLockFreedOnSessionLoss(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// One node first, so it is the one holding the lock.
	startConchds(t, ctx, endpoints, 1, logger)
	putExclusiveJob(t, ctx, cli, "excl", "@every 2s", "2m", "sleep 60")

	lockKey := core.CronLockKey("excl")
	firstLease := waitLockHeld(t, cli, lockKey, 0, 10*time.Second)

	// Second node, then "crash" the first by revoking its session lease.
	startConchds(t, ctx, endpoints, 1, logger)
	if _, err := cli.Revoke(context.Background(), clientv3.LeaseID(firstLease)); err != nil {
		t.Fatalf("failed to revoke first holder's lease: %v", err)
	}

	second := waitLockHeld(t, cli, lockKey, firstLease, 15*time.Second)
	t.Logf("lock moved from lease %x to %x", firstLease, second)
	cancel()
}

// waitLockHeld polls until key exists on a lease other than notLease and
// returns that lease.
func waitLockHeld(t *testing.T, cli *clientv3.Client, key string, notLease int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := cli.Get(context.Background(), key)
		if err == nil && len(resp.Kvs) == 1 && resp.Kvs[0].Lease != 0 && resp.Kvs[0].Lease != notLease {
			return resp.Kvs[0].Lease
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("lock %s not held (by a lease other than %x) within %s", key, notLease, timeout)
	return 0
}
