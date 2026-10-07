package core

import (
	"context"
	"testing"
	"time"
)

// TestGuardFenceFiresOnTheBound: the guardian fires when ValidUntil leaves
// budget + 1s, on its own clock, with nothing else (no loss monitor, no child)
// involved.
func TestGuardFenceFiresOnTheBound(t *testing.T) {
	budget := 500 * time.Millisecond
	k := &leaseKeeper{now: time.Now}
	start := time.Now()
	k.record(start, 0)
	k.mu.Lock()
	k.validTil = start.Add(budget + fenceSlack + 300*time.Millisecond)
	k.mu.Unlock()
	sess := &CoreSession{keeper: k}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	select {
	case <-guardFence(ctx, sess, budget):
		if took := time.Since(start); took < 250*time.Millisecond || took > 600*time.Millisecond {
			t.Fatalf("fired after %s, want ~300ms", took)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guardian never fired")
	}
}

// TestGuardFenceFollowsRenewals: a renewal that moves the bound while the
// guardian waits postpones it.
func TestGuardFenceFollowsRenewals(t *testing.T) {
	budget := 500 * time.Millisecond
	k := &leaseKeeper{now: time.Now}
	start := time.Now()
	k.record(start, 0)
	k.mu.Lock()
	k.validTil = start.Add(budget + fenceSlack + 200*time.Millisecond)
	k.mu.Unlock()
	sess := &CoreSession{keeper: k}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fire := guardFence(ctx, sess, budget)
	time.Sleep(100 * time.Millisecond)
	k.mu.Lock()
	k.validTil = time.Now().Add(budget + fenceSlack + 600*time.Millisecond) // renewed
	k.mu.Unlock()
	select {
	case <-fire:
		if took := time.Since(start); took < 600*time.Millisecond {
			t.Fatalf("fired after %s, before the renewed bound", took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("guardian never fired")
	}
}
