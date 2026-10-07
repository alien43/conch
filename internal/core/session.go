package core

import (
	"context"
	"errors"
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

	keeper *leaseKeeper
	ttl    time.Duration
	stop   context.CancelFunc
}

func NewCoreSession(ctx context.Context, endpoints []string, dialTimeout time.Duration, ttl time.Duration, logger *slog.Logger) (*CoreSession, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}

	// Grant and renew the lease ourselves, so we know when each renewal was
	// sent (see leaseKeeper). The concurrency session is built on that lease
	// for elections, semaphores and locks; its own keepalive stream keeps
	// running alongside ours. Extra renewals only extend the lease, so the
	// send-time bound stays safe.
	gctx, gcancel := context.WithTimeout(ctx, dialTimeout+ttl/3)
	keeper, err := grantLease(gctx, cli, ttl)
	gcancel()
	if err != nil {
		cli.Close()
		return nil, err
	}
	sess, err := concurrency.NewSession(cli, concurrency.WithLease(keeper.id), concurrency.WithTTL(int(ttl.Seconds())))
	if err != nil {
		_, _ = cli.Revoke(context.Background(), keeper.id)
		cli.Close()
		return nil, err
	}

	logger.Info("session acquired", "lease", fmt.Sprintf("%x", sess.Lease()), "ttl", ttl)

	// Create a sub-context that we can cancel on lease loss
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

	go monitorLease(monitorCtx, keeper, ttl, cancel, logger)

	return &CoreSession{
		Client:  cli,
		Session: sess,
		LeaseID: sess.Lease(),
		DoneCh:  doneCh,
		keeper:  keeper,
		ttl:     ttl,
		stop:    cancel,
	}, nil
}

// ValidUntil is the local-clock time until which the session's lease certainly
// exists: the send time of the newest successful renewal plus the TTL etcd
// granted. A rival cannot acquire anything held under this lease before then.
func (cs *CoreSession) ValidUntil() time.Time { return cs.keeper.ValidUntil() }

// LossAt is when the wrapper declares the lease lost unless a renewal succeeds
// first: LossDetectTimeout(ttl) after the newest successful renewal was sent.
func (cs *CoreSession) LossAt() time.Time {
	return cs.keeper.LastSent().Add(LossDetectTimeout(cs.ttl))
}

func (cs *CoreSession) Close() {
	if cs.stop != nil {
		cs.stop()
	}
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
// Both sides count from the same instant, the send time of the newest
// successful renewal: the wrapper declares loss LossDetectTimeout(ttl) after
// it (monitorLease), and etcd cannot expire the lease before ttl after it
// (leaseKeeper). The 1s covers clock-rate differences and scheduling. If the
// rule is broken and killAfter was not given explicitly, a shorter delay is
// returned. Otherwise killAfter is returned unchanged with a warning to log.
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

// monitorLease renews the lease and cancels the session once it counts as
// lost: etcd says it is gone, or no renewal sent in the last
// LossDetectTimeout(ttl) has succeeded. Counting from the send time, not from
// when a response arrived, keeps the wrapper's clock no later than etcd's.
func monitorLease(ctx context.Context, k *leaseKeeper, ttl time.Duration, cancelFunc context.CancelFunc, logger *slog.Logger) {
	defer cancelFunc()
	lease := fmt.Sprintf("%x", k.id)
	detect := LossDetectTimeout(ttl)

	renewDone := make(chan error, 1)
	go func() {
		renewDone <- k.run(ctx, func(err error) {
			logger.Warn("lease renewal failed", "lease", lease, "err", err)
		})
	}()

	for {
		lossAt := k.LastSent().Add(detect)
		timer := time.NewTimer(time.Until(lossAt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case err := <-renewDone:
			timer.Stop()
			if errors.Is(err, errLeaseGone) {
				logger.Warn("lease not found", "lease", lease)
			}
			return
		case <-timer.C:
			if k.LastSent().Add(detect).After(k.now()) {
				continue // a renewal succeeded while we waited
			}
			logger.Warn("lease renewal timeout (no successful renewal)", "lease", lease, "timeout", detect)
			return
		}
	}
}
