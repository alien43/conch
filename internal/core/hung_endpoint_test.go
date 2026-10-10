package core

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"
)

// TestSessionSurvivesOneHungEndpoint: the session's client is connected
// through an endpoint that starts dropping every byte (a member that hangs,
// or a NIC that died) while another endpoint still works. Renewals must keep
// succeeding through the other one: no lease loss, so no fence of a healthy
// term. Without per-endpoint renewal, the single client keeps sending into the
// dead connection and the lease is declared lost after LossDetectTimeout.
func TestSessionSurvivesOneHungEndpoint(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	defer etcd.Stop()
	px, err := testutil.StartProxy(etcd.ClientURL)
	if err != nil {
		t.Fatal(err)
	}
	defer px.Close()

	ttl := 6 * time.Second
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// The proxy first, so the session's main client is connected through it.
	s, err := NewCoreSession(context.Background(), []string{px.Addr(), etcd.ClientURL}, 2*time.Second, ttl, log)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	px.Blackhole(true)
	hung := time.Now()
	window := 3 * LossDetectTimeout(ttl)
	select {
	case <-s.DoneCh:
		t.Fatalf("session lost %v after one endpoint hung, with another one healthy", time.Since(hung).Round(time.Millisecond))
	case <-time.After(window):
	}
	if age := time.Since(s.keeper.LastSent()); age > ttl/3+time.Second {
		t.Fatalf("last successful renewal %v ago: renewals stopped", age.Round(time.Millisecond))
	}
	t.Logf("session alive %v after one endpoint hung; last renewal %v ago", window, time.Since(s.keeper.LastSent()).Round(time.Millisecond))
}
