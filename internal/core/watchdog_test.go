package core

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNotify is a NOTIFY_SOCKET that records what is sent to it.
type fakeNotify struct {
	path string
	mu   sync.Mutex
	msgs []string
}

func newFakeNotify(t *testing.T) *fakeNotify {
	t.Helper()
	f := &fakeNotify{path: filepath.Join(t.TempDir(), "notify.sock")}
	c, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: f.path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	go func() {
		buf := make([]byte, 256)
		for {
			n, _, err := c.ReadFromUnix(buf)
			if err != nil {
				return
			}
			f.mu.Lock()
			f.msgs = append(f.msgs, string(buf[:n]))
			f.mu.Unlock()
		}
	}()
	return f
}

func (f *fakeNotify) pets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.msgs {
		if m == "WATCHDOG=1" {
			n++
		}
	}
	return n
}

func (f *fakeNotify) all() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.msgs, ",")
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// petsDuring counts pets sent while d elapses.
func petsDuring(f *fakeNotify, d time.Duration) int {
	before := f.pets()
	time.Sleep(d)
	return f.pets() - before
}

func sessWithBound(validUntil time.Time) *CoreSession {
	k := &leaseKeeper{now: time.Now}
	k.validTil = validUntil
	return &CoreSession{keeper: k}
}

// TestWatchdogFitnessRule walks every state of the rule.
func TestWatchdogFitnessRule(t *testing.T) {
	w := NewWatchdog("unused", time.Second, "", 0, quietLog())
	if ok, _ := w.Fit(); !ok {
		t.Fatal("idle must be fit")
	}
	budget := time.Second
	w.Holding(sessWithBound(time.Now().Add(budget+fenceSlack+time.Hour)), budget)
	if ok, why := w.Fit(); !ok {
		t.Fatalf("holding with time left must be fit: %s", why)
	}
	w.Holding(sessWithBound(time.Now().Add(budget+fenceSlack-time.Millisecond)), budget)
	if ok, _ := w.Fit(); ok {
		t.Fatal("holding past the fence start deadline must be unfit")
	}
	w.Fencing(time.Hour)
	if ok, _ := w.Fit(); !ok {
		t.Fatal("a fence within its budget must be fit")
	}
	w.Fencing(-time.Millisecond)
	if ok, _ := w.Fit(); ok {
		t.Fatal("a fence past its budget must be unfit")
	}
	w.Fenced(false)
	if ok, _ := w.Fit(); ok {
		t.Fatal("a failed fence must be unfit")
	}
	w.Released() // a failed fence is not released by the term ending
	if ok, _ := w.Fit(); ok {
		t.Fatal("Released must not clear a failed fence")
	}
	w.Fencing(time.Hour) // a retry starts
	if ok, _ := w.Fit(); ok {
		t.Fatal("a retry after a failed fence must stay unfit until it confirms")
	}
	w.Fenced(true)
	if ok, _ := w.Fit(); !ok {
		t.Fatal("a confirmed retry must make it fit again")
	}
}

// TestWatchdogHeartbeat: while holding, a stale or missing child heartbeat
// makes conch unfit, after a grace of one stale period from the term's start.
func TestWatchdogHeartbeat(t *testing.T) {
	hb := filepath.Join(t.TempDir(), "hb")
	stale := 200 * time.Millisecond
	w := NewWatchdog("unused", time.Second, hb, stale, quietLog())
	w.Holding(sessWithBound(time.Now().Add(time.Hour)), 0)
	if ok, why := w.Fit(); !ok {
		t.Fatalf("within the start grace a missing heartbeat is fine: %s", why)
	}
	time.Sleep(stale + 50*time.Millisecond)
	if ok, _ := w.Fit(); ok {
		t.Fatal("missing heartbeat after the grace must be unfit")
	}
	if err := os.WriteFile(hb, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, why := w.Fit(); !ok {
		t.Fatalf("fresh heartbeat must be fit: %s", why)
	}
	old := time.Now().Add(-time.Second)
	if err := os.Chtimes(hb, old, old); err != nil {
		t.Fatal(err)
	}
	if ok, _ := w.Fit(); ok {
		t.Fatal("stale heartbeat must be unfit")
	}
}

// TestWatchdogPetsOnlyWhileFit: the petter sends READY=1, pets while fit,
// stops when unfit and resumes when fit again.
func TestWatchdogPetsOnlyWhileFit(t *testing.T) {
	f := newFakeNotify(t)
	w := NewWatchdog(f.path, 20*time.Millisecond, "", 0, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	if n := petsDuring(f, 200*time.Millisecond); n < 5 {
		t.Fatalf("idle: %d pets in 200ms, want ≥5", n)
	}
	w.Holding(sessWithBound(time.Now().Add(time.Hour)), time.Second)
	if n := petsDuring(f, 200*time.Millisecond); n < 5 {
		t.Fatalf("holding: %d pets in 200ms, want ≥5", n)
	}
	w.Fencing(time.Hour)
	w.Fenced(false)
	time.Sleep(30 * time.Millisecond) // let an in-flight tick land
	if n := petsDuring(f, 200*time.Millisecond); n != 0 {
		t.Fatalf("failed fence: %d pets, want 0", n)
	}
	w.Fenced(true)
	if n := petsDuring(f, 200*time.Millisecond); n < 5 {
		t.Fatalf("after a confirmed retry: %d pets in 200ms, want ≥5", n)
	}
	if !strings.HasPrefix(f.all(), "READY=1,") {
		t.Fatalf("first message %q, want READY=1", f.all())
	}
}

func TestNewWatchdogFromEnv(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	t.Setenv("WATCHDOG_USEC", "")
	if w, why := NewWatchdogFromEnv("", 0, quietLog()); w != nil || why == "" {
		t.Fatal("no env must mean no watchdog, with a reason")
	}
	t.Setenv("NOTIFY_SOCKET", "/run/x")
	t.Setenv("WATCHDOG_USEC", "4000000")
	t.Setenv("WATCHDOG_PID", "1")
	if w, _ := NewWatchdogFromEnv("", 0, quietLog()); w != nil {
		t.Fatal("WATCHDOG_PID of another process must disable it")
	}
	t.Setenv("WATCHDOG_PID", "")
	w, why := NewWatchdogFromEnv("", 0, quietLog())
	if w == nil {
		t.Fatal(why)
	}
	if w.interval != 2*time.Second {
		t.Fatalf("interval %v, want half of WATCHDOG_USEC (2s)", w.interval)
	}
}
