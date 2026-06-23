package sema

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"
	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestSemaNonblockAndTimeout(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Contender 1 holds a lock (max=1)
	sess1, err := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess1.Close()

	holder1 := NewSemaHolder(sess1, "mylock", 1, false, `{"host":"host1"}`, -1)
	rev1, err := holder1.Acquire(ctx)
	if err != nil {
		t.Fatalf("contender 1 failed to acquire: %v", err)
	}
	if rev1 <= 0 {
		t.Fatalf("invalid revision for contender 1: %d", rev1)
	}

	// 2. Contender 2 tries to acquire with nonblock (waitLimit = 0)
	sess2, err := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess2.Close()

	// Run with waitLimit = 0
	code2, err := RunSema(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, "mylock", 1, false, 0, []string{"true"})
	if err != nil {
		t.Fatalf("RunSema for contender 2 failed: %v", err)
	}
	if code2 != 75 {
		t.Errorf("expected contender 2 to exit with 75 (EX_TEMPFAIL) on nonblock, got %d", code2)
	}

	// 3. Contender 3 tries to acquire with a timeout of 500ms
	code3, err := RunSema(ctx, logger, endpoints, 1*time.Second, 10*time.Second, 2*time.Second, "mylock", 1, false, 500*time.Millisecond, []string{"true"})
	if err != nil {
		t.Fatalf("RunSema for contender 3 failed: %v", err)
	}
	if code3 != 75 {
		t.Errorf("expected contender 3 to exit with 75 on timeout, got %d", code3)
	}

	// 4. Verify that contender 3's queue key is removed!
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	getResp, err := cli.Get(ctx, "/conch/v1/sema/mylock/1/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to list keys: %v", err)
	}

	// Only contender 1's key should be present!
	if len(getResp.Kvs) != 1 {
		t.Errorf("expected exactly 1 key in etcd, got %d", len(getResp.Kvs))
		for _, kv := range getResp.Kvs {
			t.Logf("key: %s", string(kv.Key))
		}
	}
}

func TestSemaPromotion(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Max capacity = 2
	sess1, _ := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	defer sess1.Close()
	holder1 := NewSemaHolder(sess1, "mysema", 2, false, `{"host":"host1"}`, -1)
	_, _ = holder1.Acquire(ctx)

	sess2, _ := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	defer sess2.Close()
	holder2 := NewSemaHolder(sess2, "mysema", 2, false, `{"host":"host2"}`, -1)
	_, _ = holder2.Acquire(ctx)

	// Now 3rd contender should block
	sess3, _ := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	defer sess3.Close()
	holder3 := NewSemaHolder(sess3, "mysema", 2, false, `{"host":"host3"}`, -1)

	acquiredCh := make(chan int64, 1)
	go func() {
		rev, _ := holder3.Acquire(ctx)
		acquiredCh <- rev
	}()

	// Verify that holder3 is currently waiting
	time.Sleep(200 * time.Millisecond)
	select {
	case <-acquiredCh:
		t.Fatalf("holder3 acquired prematurely when sema was full")
	default:
	}

	// Release holder1
	err = holder1.Release(ctx)
	if err != nil {
		t.Fatalf("failed to release holder 1: %v", err)
	}

	// Now holder3 should acquire the slot!
	select {
	case rev := <-acquiredCh:
		if rev <= 0 {
			t.Errorf("invalid rev on promotion: %d", rev)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("holder3 was not promoted after holder1 was released")
	}
}

func TestSemaMismatchedMax(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	endpoints := []string{etcd.ClientURL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess1, _ := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	defer sess1.Close()

	sess2, _ := core.NewCoreSession(ctx, endpoints, 1*time.Second, 10*time.Second, logger)
	defer sess2.Close()

	// Land in separate prefixes!
	holder1 := NewSemaHolder(sess1, "testsema", 1, false, `{"host":"host1"}`, -1)
	holder2 := NewSemaHolder(sess2, "testsema", 2, false, `{"host":"host2"}`, -1)

	_, err1 := holder1.Acquire(ctx)
	_, err2 := holder2.Acquire(ctx)

	if err1 != nil || err2 != nil {
		t.Fatalf("both should acquire successfully despite same name because max differs: %v, %v", err1, err2)
	}

	if !strings.Contains(holder1.key, "/testsema/1/") {
		t.Errorf("unexpected key for holder1: %s", holder1.key)
	}
	if !strings.Contains(holder2.key, "/testsema/2/") {
		t.Errorf("unexpected key for holder2: %s", holder2.key)
	}
}
