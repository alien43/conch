package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// ExitFenceFailed is returned when the --fence command did not confirm within
// its budget: whatever it was meant to stop may still be running.
const ExitFenceFailed = 71

// Reasons a fence runs, passed as CONCH_FENCE_REASON.
const (
	FenceReasonLeaseDeadline = "lease-deadline" // the guardian: no renewal in time
	FenceReasonLost          = "lost"           // the session reported the lease gone
	FenceReasonChildExit     = "child-exit"
	FenceReasonSignal        = "signal"
	FenceReasonCancelled     = "cancelled"
	FenceReasonRetry         = "retry" // a re-run after a failed fence (--restart)
)

// fenceSlack is kept between a fence's hard deadline and the lease's
// ValidUntil, for clock-rate differences and scheduling.
const fenceSlack = time.Second

// FitFence checks that a fence started at loss detection can finish before
// the server can expire the lease: LossDetectTimeout(ttl) + budget + 1s < ttl.
// A non-empty return is the reason to refuse to start.
func FitFence(ttl, budget time.Duration) string {
	if budget <= 0 {
		return "--fence-budget must be positive"
	}
	detect := LossDetectTimeout(ttl)
	if detect+budget+fenceSlack >= ttl {
		return fmt.Sprintf("loss detection (%v) + fence budget (%v) + %v >= ttl (%v): the fence could still be running when a rival acquires; raise --ttl or lower --fence-budget",
			detect.Round(time.Millisecond), budget, fenceSlack, ttl)
	}
	return ""
}

// FenceSpec is what a fence run needs; it outlives the session that set it.
type FenceSpec struct {
	Cmd    string
	Budget time.Duration
	Name   string
	Rev    int64
	Lease  int64
}

// RunFence runs the fence command in its own process group and waits at most
// Budget for it. Only exit 0 within the budget counts as confirmed; at the
// deadline the whole group is SIGKILLed and the fence has failed.
func RunFence(logger *slog.Logger, f FenceSpec, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), f.Budget)
	defer cancel()

	logger.Warn("fence-start", "name", f.Name, "rev", f.Rev, "reason", reason, "budget", f.Budget)
	start := time.Now()

	cmd := exec.Command("/bin/sh", "-c", f.Cmd)
	cmd.SysProcAttr = childProcAttr()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("CONCH_NAME=%s", f.Name),
		fmt.Sprintf("CONCH_REV=%d", f.Rev),
		fmt.Sprintf("CONCH_LEASE=%x", f.Lease),
		fmt.Sprintf("CONCH_FENCE_REASON=%s", reason),
	)
	if err := cmd.Start(); err != nil {
		logger.Error("fence-failed", "name", f.Name, "rev", f.Rev, "err", err)
		return err
	}
	pgid := cmd.Process.Pid
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			logger.Error("fence-failed", "name", f.Name, "rev", f.Rev, "err", err, "took", time.Since(start).Round(time.Millisecond))
			return err
		}
		logger.Info("fence-confirmed", "name", f.Name, "rev", f.Rev, "took", time.Since(start).Round(time.Millisecond))
		return nil
	case <-ctx.Done():
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-done
		logger.Error("fence-failed", "name", f.Name, "rev", f.Rev, "err", "budget exceeded", "budget", f.Budget)
		return errors.New("fence budget exceeded")
	}
}

// guardFence closes the returned channel once the lease's validity bound
// leaves less than budget + fenceSlack: the last moment a fence can start and
// still finish before a rival can acquire. It runs on conch's own clock and
// does not depend on the child, the session's done channel or the loss
// monitor. With FitFence satisfied, loss detection normally fires first; this
// is the backstop.
func guardFence(ctx context.Context, sess *CoreSession, budget time.Duration) <-chan struct{} {
	fire := make(chan struct{})
	go func() {
		for {
			startBy := sess.ValidUntil().Add(-budget - fenceSlack)
			wait := time.Until(startBy)
			if wait <= 0 {
				close(fire)
				return
			}
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
				// A renewal may have moved the bound while we slept; loop re-reads it.
			}
		}
	}()
	return fire
}
