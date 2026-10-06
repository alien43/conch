package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type CoreSession struct {
	Client  *clientv3.Client
	Session *concurrency.Session
	LeaseID clientv3.LeaseID
	DoneCh  <-chan struct{}
}

func NewCoreSession(ctx context.Context, endpoints []string, dialTimeout time.Duration, ttl time.Duration, logger *slog.Logger) (*CoreSession, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}

	// Create concurrency session
	sess, err := concurrency.NewSession(cli, concurrency.WithTTL(int(ttl.Seconds())))
	if err != nil {
		cli.Close()
		return nil, err
	}

	logger.Info("session acquired", "lease", fmt.Sprintf("%x", sess.Lease()), "ttl", ttl)

	// Create a sub-context that we can cancel on keepalive failure
	monitorCtx, cancel := context.WithCancel(ctx)

	// Create a combined Done channel
	doneCh := make(chan struct{})

	go func() {
		select {
		case <-sess.Done():
			logger.Warn("session done channel closed")
		case <-monitorCtx.Done():
		}
		close(doneCh)
	}()

	// Start our keepalive monitor
	go monitorKeepAlive(monitorCtx, cli, sess.Lease(), ttl, cancel, logger)

	return &CoreSession{
		Client:  cli,
		Session: sess,
		LeaseID: sess.Lease(),
		DoneCh:  doneCh,
	}, nil
}

func (cs *CoreSession) Close() {
	if cs.Session != nil {
		_ = cs.Session.Close()
	}
	if cs.Client != nil {
		_ = cs.Client.Close()
	}
}

// LossDetectTimeout is how long the wrapper goes without a keepalive response
// before declaring the lease lost: one keepalive interval (TTL/3) plus a margin.
func LossDetectTimeout(ttl time.Duration) time.Duration {
	interval := ttl / 3
	margin := 1500 * time.Millisecond
	if interval > 2*time.Second {
		margin = interval / 2
	}
	return interval + margin
}

// DefaultKillAfter is the SIGTERM -> SIGKILL delay when none is given and it
// fits the TTL (see FitKillAfter).
const DefaultKillAfter = 5 * time.Second

// killAfterSlack is the minimum margin kept between our SIGKILL and the
// earliest moment the server can expire the lease and a rival can start.
const killAfterSlack = time.Second

// FitKillAfter checks that a child ignoring SIGTERM is SIGKILLed before the
// server can expire our lease: LossDetectTimeout(ttl) + killAfter + 1s < ttl.
// The server expires the lease one TTL after the last renewal it received;
// we start counting from the last response we received, so the two clocks
// start together. If the rule is broken and killAfter was not given
// explicitly, a shorter delay is returned. Otherwise killAfter is returned
// unchanged with a warning to log.
func FitKillAfter(ttl, killAfter time.Duration, explicit bool) (time.Duration, string) {
	detect := LossDetectTimeout(ttl)
	fits := func(ka time.Duration) bool { return detect+ka+killAfterSlack < ttl }
	if fits(killAfter) {
		return killAfter, ""
	}
	if !explicit {
		lowered := (ttl - detect) / 2
		if lowered >= 500*time.Millisecond && fits(lowered) {
			return lowered, ""
		}
	}
	return killAfter, fmt.Sprintf("loss detection (%v) + kill-after (%v) + %v >= ttl (%v): a child that ignores SIGTERM may still be running when a rival acquires; raise --ttl or lower --kill-after", detect.Round(time.Millisecond), killAfter, killAfterSlack, ttl)
}

func monitorKeepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, ttl time.Duration, cancelFunc context.CancelFunc, logger *slog.Logger) {
	ch, err := cli.KeepAlive(ctx, leaseID)
	if err != nil {
		logger.Error("failed to start keepalive monitor stream", "err", err)
		cancelFunc()
		return
	}

	timeout := LossDetectTimeout(ttl)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				logger.Warn("keepalive channel closed by client", "lease", fmt.Sprintf("%x", leaseID))
				cancelFunc()
				return
			}
			if resp == nil {
				logger.Warn("keepalive channel returned nil response", "lease", fmt.Sprintf("%x", leaseID))
				cancelFunc()
				return
			}
			// Reset timer
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		case <-timer.C:
			logger.Warn("keepalive response timeout (missed keepalive)", "lease", fmt.Sprintf("%x", leaseID), "timeout", timeout)
			cancelFunc()
			return
		}
	}
}
