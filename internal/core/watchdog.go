package core

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Watchdog pets systemd's service watchdog (sd_notify WATCHDOG=1) only while
// conch is fit, so that WatchdogSec= (and FailureAction=, if the unit sets
// one) acts on a conch that can no longer guarantee its fence:
//
//   - not holding, and no fence left unconfirmed: fit;
//   - holding: fit until the fence's start deadline (ValidUntil − budget − 1s)
//     passes without a renewal, and, with a heartbeat file, while the child
//     keeps it fresh;
//   - fencing: fit until the fence's budget runs out;
//   - a fence failed: unfit until a retry confirms.
//
// One process pets. With NotifyAccess=all a child's pets would reset the same
// timer and hide a wedged conch, and conch's would hide a wedged child; the
// child reports through the heartbeat file instead.
type Watchdog struct {
	socket    string
	interval  time.Duration // pet period: WATCHDOG_USEC / 2
	heartbeat string
	stale     time.Duration
	log       *slog.Logger

	mu        sync.Mutex
	state     wdState
	sess      *CoreSession
	budget    time.Duration
	termStart time.Time
	fenceBy   time.Time

	now func() time.Time
}

type wdState int

const (
	wdIdle wdState = iota
	wdHolding
	wdFencing
	wdFenceFailed
)

// NewWatchdogFromEnv reads NOTIFY_SOCKET and WATCHDOG_USEC (and WATCHDOG_PID,
// if set, must be ours). It returns nil and a reason when systemd has not
// enabled a watchdog for this process.
func NewWatchdogFromEnv(heartbeat string, stale time.Duration, log *slog.Logger) (*Watchdog, string) {
	sock := os.Getenv("NOTIFY_SOCKET")
	usecStr := os.Getenv("WATCHDOG_USEC")
	if sock == "" || usecStr == "" {
		return nil, "NOTIFY_SOCKET or WATCHDOG_USEC not set (no WatchdogSec= on the unit?)"
	}
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return nil, "WATCHDOG_PID is not this process"
	}
	usec, err := strconv.ParseInt(usecStr, 10, 64)
	if err != nil || usec <= 0 {
		return nil, fmt.Sprintf("bad WATCHDOG_USEC %q", usecStr)
	}
	return NewWatchdog(sock, time.Duration(usec)*time.Microsecond/2, heartbeat, stale, log), ""
}

// NewWatchdog pets socket every interval while fit.
func NewWatchdog(socket string, interval time.Duration, heartbeat string, stale time.Duration, log *slog.Logger) *Watchdog {
	return &Watchdog{socket: socket, interval: interval, heartbeat: heartbeat, stale: stale, log: log, now: time.Now}
}

// Holding: a term started under sess; budget is the fence budget (0: none).
func (w *Watchdog) Holding(sess *CoreSession, budget time.Duration) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state, w.sess, w.budget, w.termStart = wdHolding, sess, budget, w.now()
}

// Fencing: a fence with this budget starts now. A retry after a failed fence
// stays unfit: only its success (Fenced(true)) clears the failure.
func (w *Watchdog) Fencing(budget time.Duration) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == wdFenceFailed {
		return
	}
	w.state, w.fenceBy = wdFencing, w.now().Add(budget)
}

// Fenced records a fence's result.
func (w *Watchdog) Fenced(ok bool) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ok {
		w.state = wdIdle
	} else {
		w.state = wdFenceFailed
	}
	w.sess = nil
}

// Released: the term ended with nothing left to fence.
func (w *Watchdog) Released() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != wdFenceFailed {
		w.state, w.sess = wdIdle, nil
	}
}

// Fit reports whether conch may pet, and why not.
func (w *Watchdog) Fit() (bool, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	switch w.state {
	case wdFenceFailed:
		return false, "fence failed and not yet confirmed by a retry"
	case wdFencing:
		if !now.Before(w.fenceBy) {
			return false, "fence still running past its budget"
		}
		return true, ""
	case wdHolding:
		if w.budget > 0 && w.sess != nil && !now.Before(w.sess.ValidUntil().Add(-w.budget-fenceSlack)) {
			return false, "lease bound passed the fence deadline"
		}
		if w.heartbeat != "" && now.Sub(w.termStart) > w.stale {
			st, err := os.Stat(w.heartbeat)
			if err != nil {
				return false, "child heartbeat missing: " + err.Error()
			}
			if age := now.Sub(st.ModTime()); age > w.stale {
				return false, fmt.Sprintf("child heartbeat stale (%s)", age.Round(time.Millisecond))
			}
		}
		return true, ""
	}
	return true, ""
}

// Run sends READY=1, then WATCHDOG=1 every interval while fit, until ctx ends.
func (w *Watchdog) Run(ctx context.Context) {
	if w == nil {
		return
	}
	_ = w.notify("READY=1")
	t := time.NewTicker(w.interval)
	defer t.Stop()
	starving := false
	for {
		if ok, why := w.Fit(); ok {
			if starving {
				w.log.Info("watchdog-resume")
				starving = false
			}
			if err := w.notify("WATCHDOG=1"); err != nil {
				w.log.Warn("watchdog-notify-failed", "err", err)
			}
		} else if !starving {
			w.log.Error("watchdog-starve", "reason", why)
			starving = true
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Watchdog) notify(msg string) error {
	addr := w.socket
	if len(addr) > 0 && addr[0] == '@' {
		addr = "\x00" + addr[1:]
	}
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.Write([]byte(msg))
	return err
}
