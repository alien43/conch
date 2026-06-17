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

func monitorKeepAlive(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, ttl time.Duration, cancelFunc context.CancelFunc, logger *slog.Logger) {
	ch, err := cli.KeepAlive(ctx, leaseID)
	if err != nil {
		logger.Error("failed to start keepalive monitor stream", "err", err)
		cancelFunc()
		return
	}

	// Keepalive interval is ttl / 3
	interval := ttl / 3
	margin := 1500 * time.Millisecond
	if interval > 2*time.Second {
		margin = interval / 2
	}
	timeout := interval + margin

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
