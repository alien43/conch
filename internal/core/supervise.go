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
}

func runHook(ctx context.Context, logger *slog.Logger, cmdStr string, hookName string, name string, rev int64, leaseID int64, timeout time.Duration) error {
	if cmdStr == "" {
		return nil
	}

	hookCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logger.Info("running-hook", "hook", hookName, "name", name, "cmd", cmdStr, "rev", rev)

	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
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
		logger.Warn("hook-timeout", "hook", hookName, "name", name, "rev", rev)
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

	// Ensure we release the hold on exit
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := hold.Release(releaseCtx); err != nil {
			logger.Warn("error during release", "name", hold.Name(), "err", err)
		}
		logger.Info("released", "name", hold.Name(), "key", hold.Key(), "rev", rev)
	}()

	childStarted := false
	defer func() {
		if childStarted && cfg.OnLose != "" {
			_ = runHook(context.Background(), logger, cfg.OnLose, "on-lose", hold.Name(), rev, int64(sess.LeaseID), cfg.HookTimeout)
		}
	}()

	// Run on-acquire hook
	if cfg.OnAcquire != "" {
		if err := runHook(ctx, logger, cfg.OnAcquire, "on-acquire", hold.Name(), rev, int64(sess.LeaseID), cfg.HookTimeout); err != nil {
			return 75, OutcomeAcquireFailed, err
		}
	}

	// If no command is provided, we just exit with 0 immediately after acquiring and releasing
	if len(cmdArgs) == 0 {
		return 0, OutcomeExitNormal, nil
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Set env vars
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("CONCH_NAME=%s", hold.Name()),
		fmt.Sprintf("CONCH_REV=%d", rev),
		fmt.Sprintf("CONCH_LEASE=%x", sess.LeaseID),
	)

	logger.Info("child-start", "name", hold.Name(), "cmd", strings.Join(cmdArgs, " "), "rev", rev)
	if err := cmd.Start(); err != nil {
		logger.Error("failed to start child", "name", hold.Name(), "err", err)
		return 1, OutcomeExitNormal, err
	}

	childStarted = true
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
		return exitCode, OutcomeExitNormal, nil

	case <-ctx.Done():
		logger.Warn("context-cancelled", "name", hold.Name(), "err", ctx.Err(), "rev", rev)
		_ = terminateGroup(logger, hold.Name(), rev, pgid, killAfter, childDone)
		return 70, OutcomeContextCancelled, nil

	case <-sess.DoneCh:
		logger.Warn("lost", "name", hold.Name(), "key", hold.Key(), "rev", rev)
		_ = terminateGroup(logger, hold.Name(), rev, pgid, killAfter, childDone)
		return 70, OutcomeHoldLost, nil

	case sig := <-sigChan:
		logger.Info("wrapper-signal", "name", hold.Name(), "signal", sig.String(), "rev", rev)
		err := terminateGroup(logger, hold.Name(), rev, pgid, killAfter, childDone)
		exitCode := getExitCode(err)
		logger.Info("child-exit", "name", hold.Name(), "exit", exitCode, "rev", rev)
		return exitCode, OutcomeSignalReceived, nil
	}
}
