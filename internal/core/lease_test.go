package core

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/conch/internal/testutil"
)

// TestValidityBoundNeverExceedsServerExpiry is the property the wrapper's kill
// deadline rests on: the locally computed ValidUntil, and the earlier LossAt,
// are never later than the moment etcd actually expires the lease. Each round
// renews for a while, then cuts the session off from etcd and watches a key
// attached to the lease from a second, direct client.
func TestValidityBoundNeverExceedsServerExpiry(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()
	observer, err := clientv3.New(clientv3.Config{Endpoints: []string{etcd.ClientURL}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const rounds = 6
	ttl := 3 * time.Second
	for round := 1; round <= rounds; round++ {
		px, err := testutil.StartProxy(strings.TrimPrefix(etcd.ClientURL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		sess, err := NewCoreSession(ctx, []string{px.Addr()}, time.Second, ttl, logger)
		if err != nil {
			t.Fatal(err)
		}
		// A key attached to the lease vanishes exactly when a rival could take
		// what the lease holds. (TimeToLive truncates to whole seconds.)
		if _, err := observer.Put(context.Background(), "/probe", "x", clientv3.WithLease(sess.LeaseID)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3500 * time.Millisecond) // three renewals

		px.Blackhole(true)
		bound, lossAt := sess.ValidUntil(), sess.LossAt()

		var expired, done time.Time
		for deadline := time.Now().Add(3 * ttl); time.Now().Before(deadline) && expired.IsZero(); time.Sleep(5 * time.Millisecond) {
			if done.IsZero() {
				select {
				case <-sess.DoneCh:
					done = time.Now()
				default:
				}
			}
			r, err := observer.Get(context.Background(), "/probe")
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Kvs) == 0 {
				expired = time.Now()
			}
		}
		if expired.IsZero() {
			t.Fatalf("round %d: lease never expired", round)
		}
		if done.IsZero() {
			t.Fatalf("round %d: session not declared lost before the server expired the lease", round)
		}
		if bound.After(expired) {
			t.Fatalf("round %d: ValidUntil %s is AFTER server expiry %s", round, bound.Format(time.StampMilli), expired.Format(time.StampMilli))
		}
		if !lossAt.Before(bound) {
			t.Fatalf("round %d: LossAt %s not before ValidUntil %s", round, lossAt.Format(time.StampMilli), bound.Format(time.StampMilli))
		}
		t.Logf("round %d: slack ValidUntil→expiry %s; loss declared %s before expiry (LossAt %s before)",
			round, expired.Sub(bound).Round(time.Millisecond), expired.Sub(done).Round(time.Millisecond), expired.Sub(lossAt).Round(time.Millisecond))

		cancel()
		sess.Close()
		px.Close()
	}
}

// TestLeaseKeeperRecordIsMonotonic: a late-arriving response for an older
// renewal must never move the bound backwards.
func TestLeaseKeeperRecordIsMonotonic(t *testing.T) {
	k := &leaseKeeper{now: time.Now}
	t0 := time.Now()
	k.record(t0.Add(2*time.Second), 10)
	k.record(t0, 10) // older renewal answered later
	if got, want := k.ValidUntil(), t0.Add(12*time.Second); !got.Equal(want) {
		t.Fatalf("ValidUntil = %v, want %v", got, want)
	}
	if got, want := k.LastSent(), t0.Add(2*time.Second); !got.Equal(want) {
		t.Fatalf("LastSent = %v, want %v", got, want)
	}
}

// TestKillDeadlineSurvivesClockSkew: with the local clock running 1% slow or
// fast against etcd's, a child that ignores SIGTERM is still SIGKILLed before
// the server can expire the lease, for every TTL whose kill-after FitKillAfter
// accepts. Both counts start at the same send instant (leaseKeeper), so only
// the rate difference matters, and the rule's 1s slack covers 1% of any TTL up
// to 100s.
func TestKillDeadlineSurvivesClockSkew(t *testing.T) {
	for _, ttl := range []time.Duration{3 * time.Second, 6 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second, 90 * time.Second} {
		ka, warning := FitKillAfter(ttl, DefaultKillAfter, false)
		if warning != "" {
			continue // the wrapper warns; nothing is promised
		}
		local := LossDetectTimeout(ttl) + ka // local-clock time from send to SIGKILL
		for _, rate := range []float64{0.99, 1.01} {
			// A local clock running at `rate` reaches `local` after local/rate
			// of real time.
			real := time.Duration(float64(local) / rate)
			if real >= ttl {
				t.Errorf("ttl %v, rate %.2f: SIGKILL at real %v, not before expiry at %v", ttl, rate, real, ttl)
			}
		}
	}
}
