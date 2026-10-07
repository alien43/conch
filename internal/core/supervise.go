package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Holder interface {
	Acquire(ctx context.Context) (int64, error)
	Release(ctx context.Context) error
	Name() string
	Key() string
}

type HolderJSON struct {
	Host    string `json:"host"`
	Pid     int    `json:"pid"`
	Started string `json:"started"`
	Cmd     string `json:"cmd,omitempty"`
}

func NewHolderJSON(cmdArgs []string) ([]byte, error) {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	var cmdStr string
	if len(cmdArgs) > 0 {
		cmdStr = strings.Join(cmdArgs, " ")
		if len(cmdStr) > 256 {
			cmdStr = cmdStr[:256]
		}
	}

	h := HolderJSON{
		Host:    host,
		Pid:     os.Getpid(),
		Started: time.Now().UTC().Format(time.RFC3339),
		Cmd:     cmdStr,
	}

	return json.Marshal(h)
}

func getExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				return 128 + int(status.Signal())
			}
			return status.ExitStatus()
		}
		return exitError.ExitCode()
	}
	// Other error, e.g. executable not found
	return 1
}

type Outcome string

const (
	OutcomeExitNormal       Outcome = "exit-normal"
	OutcomeHoldLost         Outcome = "hold-lost"
	OutcomeSignalReceived   Outcome = "signal-received"
	OutcomeContextCancelled Outcome = "context-cancelled"
	OutcomeAcquireFailed    Outcome = "acquire-failed"
	OutcomeFenceFailed      Outcome = "fence-failed"
)

func terminateGroup(logger *slog.Logger, name string, rev int64, pgid int, killAfter time.Duration, childDone chan error) error {
	logger.Warn("term-sent", "name", name, "pgid", pgid, "rev", rev)
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	killTimer := time.NewTimer(killAfter)
	defer killTimer.Stop()

	select {
	case err := <-childDone:
		return err
	case <-killTimer.C:
		logger.Warn("kill-sent", "name", name, "pgid", pgid, "rev", rev)
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		err := <-childDone // wait for reap
		return err
	}
}

func Run(ctx context.Context, logger *slog.Logger, sess *CoreSession, hold Holder, cmdArgs []string, killAfter time.Duration) (int, Outcome, error) {
	return RunWithConfig(ctx, logger, sess, hold, cmdArgs, killAfter, RunConfig{})
}

type RunConfig struct {
	OnAcquire   string
	OnLose      string
	HookTimeout time.Duration

	// Fence, if set, runs whenever a term ends after on-acquire or the child
	// started, with FenceBudget as its hard deadline (see fence.go). It is
	// started by conch's own guardian at the latest when the lease's validity
	// bound leaves FenceBudget + 1s, and runs alongside the child's
	// termination, before the office is resigned.
	Fence       string
	FenceBudget time.Duration
	// OnFenceFailed is told about a fence that did not confirm, with the
	// reason it ran, so a caller can retry it before campaigning again.
	OnFenceFailed func(spec FenceSpec, reason string)
}

func runHook(ctx context.Context, logger *slog.Logger, cmdStr string, hookName string, name string, rev int64, leaseID int64, timeout time.Duration) error {
	if cmdStr == "" {
		return nil
	}

	hookCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logger.Info("running-hook", "hook", hookName, "name", name, "cmd", cmdStr, "rev", rev)

	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	cmd.SysProcAttr = childProcAttr()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("CONCH_NAME=%s", name),
		fmt.Sprintf("CONCH_REV=%d", rev),
		fmt.Sprintf("CONCH_LEASE=%x", leaseID),
	)

	if err := cmd.Start(); err != nil {
		logger.Error("hook-failed-start", "hook", hookName, "name", name, "err", err, "rev", rev)
		return err
	}

	pgid := cmd.Process.Pid

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-hookCtx.Done():
		if errors.Is(hookCtx.Err(), context.DeadlineExceeded) {
			logger.Warn("hook-timeout", "hook", hookName, "name", name, "rev", rev)
		} else {
			logger.Warn("hook-cancelled", "hook", hookName, "name", name, "rev", rev)
		}
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-done
		return hookCtx.Err()
	case err := <-done:
		if err != nil {
			logger.Error("hook-failed", "hook", hookName, "name", name, "err", err, "rev", rev)
			return err
		}
		logger.Info("hook-success", "hook", hookName, "name", name, "rev", rev)
		return nil
	}
}

func RunWithConfig(ctx context.Context, logger *slog.Logger, sess *CoreSession, hold Holder, cmdArgs []string, killAfter time.Duration, cfg RunConfig) (int, Outcome, error) {
	logger.Info("acquiring", "name", hold.Name(), "key", hold.Key())

	rev, err := hold.Acquire(ctx)
	if err != nil {
		// If context was cancelled (e.g. timeout), return 75
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Error("acquire timed out or cancelled", "name", hold.Name(), "err", err)
			return 75, OutcomeAcquireFailed, nil
		}
		logger.Error("failed to acquire hold", "name", hold.Name(), "err", err)
		return 75, OutcomeAcquireFailed, err
	}

	logger.Info("acquired", "name", hold.Name(), "key", hold.Key(), "rev", rev)

	fenceOn := cfg.Fence != ""
	spec := FenceSpec{Cmd: cfg.Fence, Budget: cfg.FenceBudget, Name: hold.Name(), Rev: rev, Lease: int64(sess.LeaseID)}
	var guard <-chan struct{} // nil, so never ready, without a fence
	if fenceOn {
		gctx, gstop := context.WithCancel(context.Background())
		defer gstop()
		guard = guardFence(gctx, sess, cfg.FenceBudget)
	}
	fenceFailed := false

	// Ensure we release the hold on exit
	defer func() {
		if fenceFailed {
			// Resigning would let a rival start while what we fence may still
			// run. Leave the office to the lease, which expires on its own.
			logger.Error("not-resigning", "name", hold.Name(), "rev", rev, "reason", "fence failed")
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := hold.Release(releaseCtx); err != nil {
			logger.Warn("error during release", "name", hold.Name(), "err", err)
		}
		logger.Info("released", "name", hold.Name(), "key", hold.Key(), "rev", rev)
	}()

	// on-lose undoes on-acquire as well as the child, so it runs if either
	// was started: a promote with no matching demote is the worst outcome.
	actedAsHolder := false
	defer func() {
		if actedAsHolder && cfg.OnLose != "" {
			_ = runHook(context.Background(), logger, cfg.OnLose, "on-lose", hold.Name(), rev, int64(sess.LeaseID), cfg.HookTimeout)
		}
	}()

	// fence runs the fence once if this term acted as the holder.
	fence := func(reason string) {
		if !fenceOn || !actedAsHolder {
			return
		}
		if err := RunFence(logger, spec, reason); err != nil {
			fenceFailed = true
			if cfg.OnFenceFailed != nil {
				cfg.OnFenceFailed(spec, reason)
			}
		}
	}
	// endTerm stops the child and fences concurrently: the fence's deadline is
	// the lease's, not the child's, so it must not wait for kill-after.
	endTerm := func(reason string, pgid int, childDone chan error) error {
		fenced := make(chan struct{})
		go func() { fence(reason); close(fenced) }()
		err := terminateGroup(logger, hold.Name(), rev, pgid, killAfter, childDone)
		<-fenced
		return err
	}
	fenceResult := func(code int, outcome Outcome) (int, Outcome, error) {
		if fenceFailed {
			return ExitFenceFailed, OutcomeFenceFailed, nil
		}
		return code, outcome, nil
	}

	// Run on-acquire hook, supervised by the lease like the child is.
	if cfg.OnAcquire != "" {
		hookCtx, stop := context.WithCancel(ctx)
		go func() {
			select {
			case <-sess.DoneCh:
				stop()
			case <-guard:
				stop()
			case <-hookCtx.Done():
			}
		}()
		actedAsHolder = true
		err := runHook(hookCtx, logger, cfg.OnAcquire, "on-acquire", hold.Name(), rev, int64(sess.LeaseID), cfg.HookTimeout)
		stop()
		if err != nil {
			select {
			case <-sess.DoneCh:
				logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev)
				fence(FenceReasonLost)
				return fenceResult(70, OutcomeHoldLost)
			case <-guard:
				logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev, "reason", "lease deadline")
				fence(FenceReasonLeaseDeadline)
				return fenceResult(70, OutcomeHoldLost)
			default:
			}
			fence(FenceReasonChildExit)
			if fenceFailed {
				return fenceResult(75, OutcomeAcquireFailed)
			}
			return 75, OutcomeAcquireFailed, err
		}
	}

	// If no command is provided, we just exit with 0 immediately after acquiring and releasing
	if len(cmdArgs) == 0 {
		fence(FenceReasonChildExit)
		return fenceResult(0, OutcomeExitNormal)
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.SysProcAttr = childProcAttr()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Set env vars
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("CONCH_NAME=%s", hold.Name()),
		fmt.Sprintf("CONCH_REV=%d", rev),
		fmt.Sprintf("CONCH_LEASE=%x", sess.LeaseID),
	)

	// The lease may have been lost while on-acquire ran; don't start a child
	// we can no longer supervise as the holder.
	select {
	case <-sess.DoneCh:
		logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev)
		fence(FenceReasonLost)
		return fenceResult(70, OutcomeHoldLost)
	case <-guard:
		logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev, "reason", "lease deadline")
		fence(FenceReasonLeaseDeadline)
		return fenceResult(70, OutcomeHoldLost)
	case <-ctx.Done():
		logger.Warn("context-cancelled", "name", hold.Name(), "err", ctx.Err(), "rev", rev)
		fence(FenceReasonCancelled)
		return fenceResult(70, OutcomeContextCancelled)
	default:
	}

	logger.Info("child-start", "name", hold.Name(), "cmd", strings.Join(cmdArgs, " "), "rev", rev)
	if err := cmd.Start(); err != nil {
		logger.Error("failed to start child", "name", hold.Name(), "err", err)
		return 1, OutcomeExitNormal, err
	}
	actedAsHolder = true

	pgid := cmd.Process.Pid

	childDone := make(chan error, 1)
	go func() {
		childDone <- cmd.Wait()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	select {
	case err := <-childDone:
		exitCode := getExitCode(err)
		logger.Info("child-exit", "name", hold.Name(), "exit", exitCode, "rev", rev)
		fence(FenceReasonChildExit)
		return fenceResult(exitCode, OutcomeExitNormal)

	case <-ctx.Done():
		logger.Warn("context-cancelled", "name", hold.Name(), "err", ctx.Err(), "rev", rev)
		_ = endTerm(FenceReasonCancelled, pgid, childDone)
		return fenceResult(70, OutcomeContextCancelled)

	case <-sess.DoneCh:
		logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev)
		_ = endTerm(FenceReasonLost, pgid, childDone)
		return fenceResult(70, OutcomeHoldLost)

	case <-guard:
		logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev, "reason", "lease deadline")
		_ = endTerm(FenceReasonLeaseDeadline, pgid, childDone)
		return fenceResult(70, OutcomeHoldLost)

	case sig := <-sigChan:
		logger.Info("wrapper-signal", "name", hold.Name(), "signal", sig.String(), "rev", rev)
		err := endTerm(FenceReasonSignal, pgid, childDone)
		exitCode := getExitCode(err)
		logger.Info("child-exit", "name", hold.Name(), "exit", exitCode, "rev", rev)
		return fenceResult(exitCode, OutcomeSignalReceived)
	}
}
