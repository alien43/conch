package elect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/alien43/conch/internal/core"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ElectHolder struct {
	sess      *core.CoreSession
	office    string
	val       string
	election  *concurrency.Election
	waitLimit time.Duration

	// campaignAfter (--campaign-after) delays entering the election until the
	// office has been vacant for this long; logger reports the delay.
	campaignAfter time.Duration
	logger        *slog.Logger
}

func NewElectHolder(sess *core.CoreSession, office string, val string, waitLimit time.Duration) *ElectHolder {
	return &ElectHolder{
		sess:      sess,
		office:    office,
		val:       val,
		election:  concurrency.NewElection(sess.Session, core.ElectElectionKey(office)),
		waitLimit: waitLimit,
	}
}

func (eh *ElectHolder) Acquire(ctx context.Context) (int64, error) {
	var cancel context.CancelFunc
	if eh.waitLimit > 0 {
		ctx, cancel = context.WithTimeout(ctx, eh.waitLimit)
		defer cancel()
	}

	// Campaign only waits for lower keys to be deleted; it never re-checks that
	// our own key survived. Abort the wait when the session dies, and confirm
	// the key afterwards, so a candidate whose lease expired while queued can't
	// "win" an office it no longer holds.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		select {
		case <-eh.sess.DoneCh:
			stop()
		case <-ctx.Done():
		}
	}()

	if eh.campaignAfter > 0 {
		if err := eh.waitVacant(ctx); err != nil {
			select {
			case <-eh.sess.DoneCh:
				return 0, errSessionLost
			default:
			}
			return 0, err
		}
	}

	if err := eh.election.Campaign(ctx, eh.val); err != nil {
		select {
		case <-eh.sess.DoneCh:
			return 0, errSessionLost
		default:
		}
		return 0, err
	}

	resp, err := eh.sess.Client.Get(ctx, eh.election.Key())
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 || resp.Kvs[0].CreateRevision != eh.election.Rev() {
		return 0, errSessionLost
	}
	select {
	case <-eh.sess.DoneCh:
		return 0, errSessionLost
	default:
	}
	return eh.election.Rev(), nil
}

var errSessionLost = errors.New("session lost while campaigning: our candidate key is gone")

// waitVacant returns once the office has had no key at all (no holder, no
// queued candidate) for campaignAfter without a break, holding no key of its
// own meanwhile. Any key put during the delay restarts it: whoever comes back
// within it, even briefly, goes first. It only decides when to campaign; the
// election itself is unchanged.
func (eh *ElectHolder) waitVacant(ctx context.Context) error {
	prefix := core.ElectPrefix(eh.office)
	for {
		resp, err := eh.sess.Client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil {
			return err
		}
		if resp.Count > 0 {
			if err := eh.watchUntil(ctx, prefix, resp.Header.Revision, clientv3.EventTypeDelete, 0); err != nil && !errors.Is(err, errWatchEvent) {
				return err
			}
			continue // a key went: re-read whether the office is empty now
		}
		eh.logger.Info("campaign-delay", "name", eh.office, "delay", eh.campaignAfter)
		switch err := eh.watchUntil(ctx, prefix, resp.Header.Revision, clientv3.EventTypePut, eh.campaignAfter); {
		case err == nil:
			return nil
		case errors.Is(err, errWatchEvent):
			eh.logger.Info("campaign-delay-reset", "name", eh.office)
		default:
			return err
		}
	}
}

var errWatchEvent = errors.New("watched event")

// watchUntil watches prefix from just after rev. It returns errWatchEvent at
// the first event of type typ, nil once quiet has passed without one (never,
// if quiet is 0), and errWatchEvent too when the watch breaks (compaction,
// reconnect), so the caller re-reads rather than trusting a gap.
func (eh *ElectHolder) watchUntil(ctx context.Context, prefix string, rev int64, typ mvccpb.Event_EventType, quiet time.Duration) error {
	wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
	defer cancel()
	wch := eh.sess.Client.Watch(wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(rev+1))
	var timer <-chan time.Time
	if quiet > 0 {
		t := time.NewTimer(quiet)
		defer t.Stop()
		timer = t.C
	}
	for {
		select {
		case <-timer:
			return nil
		case wr, ok := <-wch:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errWatchEvent
			}
			if wr.Err() != nil {
				return errWatchEvent
			}
			for _, ev := range wr.Events {
				if ev.Type == typ {
					return errWatchEvent
				}
			}
		}
	}
}

func (eh *ElectHolder) Release(ctx context.Context) error {
	return eh.election.Resign(ctx)
}

func (eh *ElectHolder) Name() string {
	return eh.office
}

func (eh *ElectHolder) Key() string {
	return core.ElectPrefix(eh.office)
}

func RunElect(ctx context.Context, logger *slog.Logger, endpoints []string, dialTimeout time.Duration, ttl time.Duration, killAfter time.Duration, restart bool, office string, waitLimit time.Duration, stableThreshold time.Duration, onAcquire string, onLose string, hookTimeout time.Duration, cmdArgs []string) (int, error) {
	return RunElectFenced(ctx, logger, endpoints, dialTimeout, ttl, killAfter, restart, office, waitLimit, stableThreshold, onAcquire, onLose, hookTimeout, Fence{}, cmdArgs)
}

// Fence is `--fence CMD --fence-budget D`; a zero value means no fence.
// Watchdog, if set, pets systemd only while conch is fit (`--watchdog`).
// CampaignAfter is `--campaign-after D` (see ElectHolder.waitVacant); it rides
// here to keep RunElectFenced's signature.
type Fence struct {
	Cmd           string
	Budget        time.Duration
	Watchdog      *core.Watchdog
	CampaignAfter time.Duration
}

// RunElectFenced is RunElect with a fence (core.RunConfig.Fence). A term whose
// fence failed is not resigned and its lease is not revoked; under --restart
// the fence is retried with backoff, and the office is only campaigned for
// again once a retry confirms. A fence that failed while conch was being
// stopped (signal, cancellation) is not retried: conch exits 71.
func RunElectFenced(ctx context.Context, logger *slog.Logger, endpoints []string, dialTimeout time.Duration, ttl time.Duration, killAfter time.Duration, restart bool, office string, waitLimit time.Duration, stableThreshold time.Duration, onAcquire string, onLose string, hookTimeout time.Duration, fence Fence, cmdArgs []string) (int, error) {
	backoff := core.NewBackoff(1*time.Second, 30*time.Second)

	if fence.Watchdog != nil {
		wctx, wstop := context.WithCancel(ctx)
		defer wstop()
		go fence.Watchdog.Run(wctx)
	}

	for {
		// 1. Establish session
		sess, err := core.NewCoreSession(ctx, endpoints, dialTimeout, ttl, logger)
		if err != nil {
			logger.Error("failed to create session", "err", err)
			if !restart {
				return 69, err
			}
			// Back off and retry session establishment
			sleepDur := backoff.Duration()
			logger.Info("backing off before retry", "duration", sleepDur)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(sleepDur):
				continue
			}
		}

		// 2. Build holder JSON for campaign
		holderVal, err := core.NewHolderJSON(cmdArgs)
		if err != nil {
			sess.Close()
			return 64, err
		}

		holder := NewElectHolder(sess, office, string(holderVal), waitLimit)
		holder.campaignAfter, holder.logger = fence.CampaignAfter, logger

		startTime := time.Now()

		// 3. Run core loop
		var failed *core.FenceSpec
		var failReason string
		exitCode, outcome, err := core.RunWithConfig(ctx, logger, sess, holder, cmdArgs, killAfter, core.RunConfig{
			OnAcquire:   onAcquire,
			OnLose:      onLose,
			HookTimeout: hookTimeout,
			Fence:       fence.Cmd,
			FenceBudget: fence.Budget,
			Watchdog:    fence.Watchdog,
			OnFenceFailed: func(spec core.FenceSpec, reason string) {
				failed, failReason = &spec, reason
			},
		})

		if outcome == core.OutcomeFenceFailed {
			sess.Abandon()
		} else {
			sess.Close()
		}

		if !restart {
			return exitCode, err
		}

		if outcome == core.OutcomeFenceFailed {
			if failed == nil || failReason == core.FenceReasonSignal || failReason == core.FenceReasonCancelled {
				return exitCode, err
			}
			// Still possibly running: no campaign until a fence confirms.
			fenceBackoff := core.NewBackoff(1*time.Second, 30*time.Second)
			for {
				sleepDur := fenceBackoff.Duration()
				logger.Warn("fence-retry-backoff", "name", office, "duration", sleepDur)
				select {
				case <-ctx.Done():
					return exitCode, nil
				case <-time.After(sleepDur):
				}
				fence.Watchdog.Fencing(failed.Budget)
				err := core.RunFence(logger, *failed, core.FenceReasonRetry)
				fence.Watchdog.Fenced(err == nil)
				if err == nil {
					break
				}
			}
		}

		// In restart mode, check if we should exit (signal received or context cancelled)
		if outcome == core.OutcomeSignalReceived || outcome == core.OutcomeContextCancelled {
			logger.Info("stopping elect restart loop due to signal or cancellation", "outcome", outcome)
			return exitCode, nil
		}

		select {
		case <-ctx.Done():
			return exitCode, nil
		default:
		}

		// Reset backoff if child survived stableThreshold
		if time.Since(startTime) >= stableThreshold {
			backoff.Reset()
		}

		// Back off
		sleepDur := backoff.Duration()
		logger.Info("re-campaigning backoff", "duration", sleepDur)
		select {
		case <-ctx.Done():
			return exitCode, nil
		case <-time.After(sleepDur):
		}
	}
}

func CmdWho(ctx context.Context, client *clientv3.Client, office string, useJSON bool) (int, error) {
	resp, err := client.Get(ctx, core.ElectPrefix(office),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		clientv3.WithLimit(1),
	)
	if err != nil {
		return 1, err
	}

	if len(resp.Kvs) == 0 {
		return 1, nil // No leader, exit 1
	}

	kv := resp.Kvs[0]
	c := candidate{
		Key:            string(kv.Key),
		CreateRevision: kv.CreateRevision,
		Value:          kv.Value,
	}

	printLeader(office, &c, useJSON)
	return 0, nil
}

type AssertOutput struct {
	Held bool   `json:"held"`
	Rev  int64  `json:"rev"`
	Host string `json:"host"`
}

func CmdAssert(ctx context.Context, client *clientv3.Client, office string, minRev int64, useJSON bool) (int, error) {
	resp, err := client.Get(ctx, core.ElectPrefix(office),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
		clientv3.WithLimit(1),
	)
	if err != nil {
		return 69, err
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	if len(resp.Kvs) == 0 {
		if useJSON {
			out := AssertOutput{Held: false, Rev: 0, Host: ""}
			bytes, _ := json.Marshal(out)
			fmt.Println(string(bytes))
		}
		return 1, nil
	}

	kv := resp.Kvs[0]
	var h core.HolderJSON
	if err := json.Unmarshal(kv.Value, &h); err != nil {
		if useJSON {
			out := AssertOutput{Held: false, Rev: kv.CreateRevision, Host: "unknown"}
			bytes, _ := json.Marshal(out)
			fmt.Println(string(bytes))
		}
		return 1, nil
	}

	isHeld := h.Host == hostname
	if isHeld && minRev > 0 && kv.CreateRevision < minRev {
		isHeld = false
	}

	if useJSON {
		out := AssertOutput{Held: isHeld, Rev: kv.CreateRevision, Host: h.Host}
		bytes, _ := json.Marshal(out)
		fmt.Println(string(bytes))
	}

	if isHeld {
		return 0, nil
	}
	return 1, nil
}

type candidate struct {
	Key            string
	CreateRevision int64
	Value          []byte
}

func printLeader(office string, c *candidate, useJSON bool) {
	if c == nil {
		if useJSON {
			fmt.Printf("{\"office\":\"%s\",\"leader\":null}\n", office)
		} else {
			fmt.Printf("%s -\n", office)
		}
		return
	}
	if useJSON {
		fmt.Println(string(c.Value))
	} else {
		var h core.HolderJSON
		if err := json.Unmarshal(c.Value, &h); err == nil {
			fmt.Printf("%s %s pid=%d since=%s rev=%d\n", office, h.Host, h.Pid, h.Started, c.CreateRevision)
		} else {
			fmt.Printf("%s unknown-host pid=0 since=unknown rev=%d\n", office, c.CreateRevision)
		}
	}
}

func CmdWatch(ctx context.Context, client *clientv3.Client, office string, useJSON bool) (int, error) {
	// 1. Initial list of all candidates
	resp, err := client.Get(ctx, core.ElectPrefix(office), clientv3.WithPrefix())
	if err != nil {
		return 1, err
	}

	var candidates []candidate
	for _, kv := range resp.Kvs {
		candidates = append(candidates, candidate{
			Key:            string(kv.Key),
			CreateRevision: kv.CreateRevision,
			Value:          kv.Value,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreateRevision < candidates[j].CreateRevision
	})

	var lastLeaderKey string
	var lastLeaderRev int64
	var hasLastLeader bool

	if len(candidates) > 0 {
		lastLeaderKey = candidates[0].Key
		lastLeaderRev = candidates[0].CreateRevision
		hasLastLeader = true
		printLeader(office, &candidates[0], useJSON)
	} else {
		printLeader(office, nil, useJSON)
	}

	// 2. Watch starting from Get's revision
	watchChan := client.Watch(ctx, core.ElectPrefix(office), clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))

	for {
		select {
		case <-ctx.Done():
			return 0, nil
		case wresp, ok := <-watchChan:
			if !ok {
				return 1, fmt.Errorf("watch channel closed")
			}
			if wresp.Err() != nil {
				return 1, wresp.Err()
			}

			candidates = updateCandidates(candidates, wresp.Events)

			// Sort
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].CreateRevision < candidates[j].CreateRevision
			})

			// Check if leader changed
			if len(candidates) > 0 {
				newLeader := &candidates[0]
				if !hasLastLeader || lastLeaderKey != newLeader.Key || lastLeaderRev != newLeader.CreateRevision {
					lastLeaderKey = newLeader.Key
					lastLeaderRev = newLeader.CreateRevision
					hasLastLeader = true
					printLeader(office, newLeader, useJSON)
				}
			} else {
				if hasLastLeader {
					hasLastLeader = false
					printLeader(office, nil, useJSON)
				}
			}
		}
	}
}

func updateCandidates(candidates []candidate, events []*clientv3.Event) []candidate {
	for _, ev := range events {
		key := string(ev.Kv.Key)
		if ev.Type == clientv3.EventTypeDelete {
			for i, c := range candidates {
				if c.Key == key {
					candidates = append(candidates[:i], candidates[i+1:]...)
					break
				}
			}
		} else {
			found := false
			for i, c := range candidates {
				if c.Key == key {
					candidates[i].Value = ev.Kv.Value
					candidates[i].CreateRevision = ev.Kv.CreateRevision
					found = true
					break
				}
			}
			if !found {
				candidates = append(candidates, candidate{
					Key:            key,
					CreateRevision: ev.Kv.CreateRevision,
					Value:          ev.Kv.Value,
				})
			}
		}
	}
	return candidates
}
