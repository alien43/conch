package core

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// leaseKeeper renews a lease by hand and tracks, on this host's monotonic
// clock, a bound on how long the lease is certain to exist.
//
// A renewal sent at local time t that succeeds with TTL d means etcd extended
// the lease to at least (its receipt time + d), which is no earlier than t + d
// in real time. So "validUntil = send time of the newest successful renewal +
// TTL" is never later than the moment etcd actually expires the lease, provided
// the two clocks run at about the same rate. etcd leader elections only ever
// extend leases, so they cannot invalidate the bound either.
//
// Anchoring on the send time is the point: a renewal's *response* arrives up to
// one round trip after etcd extended the lease, so a count started there is
// late by that much.
type leaseKeeper struct {
	cli       *clientv3.Client
	id        clientv3.LeaseID
	interval  time.Duration
	opTimeout time.Duration

	mu       sync.Mutex
	lastSent time.Time // send time of the newest successful renewal (monotonic)
	validTil time.Time // lastSent + granted TTL

	// now is replaceable in tests.
	now func() time.Time
}

// errLeaseGone means etcd no longer has the lease.
var errLeaseGone = errors.New("lease not found")

func grantLease(ctx context.Context, cli *clientv3.Client, ttl time.Duration) (*leaseKeeper, error) {
	k := &leaseKeeper{cli: cli, interval: ttl / 3, opTimeout: ttl / 3, now: time.Now}
	sent := k.now()
	resp, err := cli.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, err
	}
	k.id = resp.ID
	k.record(sent, resp.TTL)
	return k, nil
}

func (k *leaseKeeper) record(sent time.Time, ttlSeconds int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if sent.After(k.lastSent) {
		k.lastSent = sent
	}
	if v := sent.Add(time.Duration(ttlSeconds) * time.Second); v.After(k.validTil) {
		k.validTil = v
	}
}

// LastSent is the send time of the newest successful renewal (or the grant).
func (k *leaseKeeper) LastSent() time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lastSent
}

// ValidUntil is the local-clock time until which the lease certainly exists.
func (k *leaseKeeper) ValidUntil() time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.validTil
}

func (k *leaseKeeper) renew(ctx context.Context) error {
	sent := k.now()
	cctx, cancel := context.WithTimeout(ctx, k.opTimeout)
	defer cancel()
	resp, err := k.cli.KeepAliveOnce(cctx, k.id)
	if err == nil && resp.TTL <= 0 {
		err = rpctypes.ErrLeaseNotFound
	}
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return errLeaseGone
		}
		return err
	}
	k.record(sent, resp.TTL)
	return nil
}

// run renews every interval until ctx ends or etcd reports the lease gone,
// which it returns. Transient failures are retried on the next tick; deciding
// when the lease counts as lost is the caller's job (see monitorLease).
func (k *leaseKeeper) run(ctx context.Context, onErr func(error)) error {
	t := time.NewTicker(k.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		if err := k.renew(ctx); err != nil {
			if errors.Is(err, errLeaseGone) {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			onErr(err)
		}
	}
}
