package sema

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"conch/internal/core"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type SemaHolder struct {
	client    *clientv3.Client
	sess      *core.CoreSession
	name      string
	max       int
	spread    bool
	key       string
	val       string
	acquired  bool
	waitLimit time.Duration
}

func NewSemaHolder(sess *core.CoreSession, name string, max int, spread bool, val string, waitLimit time.Duration) *SemaHolder {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	key := core.SemaKey(name, max, spread, host, int64(sess.LeaseID))

	return &SemaHolder{
		client:    sess.Client,
		sess:      sess,
		name:      name,
		max:       max,
		spread:    spread,
		key:       key,
		val:       val,
		waitLimit: waitLimit,
	}
}

func (sh *SemaHolder) Acquire(ctx context.Context) (int64, error) {
	// Create context with our wait limit if specified
	if sh.waitLimit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, sh.waitLimit)
		defer cancel()
	}
	return sh.acquireInternal(ctx)
}

func (sh *SemaHolder) acquireInternal(ctx context.Context) (int64, error) {
	for {
		// 1. Put key with session lease in txn
		txn := sh.client.Txn(ctx).If(
			clientv3.Compare(clientv3.CreateRevision(sh.key), "=", 0),
		).Then(
			clientv3.OpPut(sh.key, sh.val, clientv3.WithLease(sh.sess.LeaseID)),
		)

		resp, err := txn.Commit()
		if err != nil {
			return 0, err
		}

		var wroteKey bool
		if !resp.Succeeded {
			if sh.spread {
				if sh.waitLimit == 0 {
					return 0, context.DeadlineExceeded // Treat non-block as immediate timeout (exit 75)
				}
				// Wait (block or timeout) for our own key to be deleted, then retry
				watchCtx, watchCancel := context.WithCancel(ctx)
				watchChan := sh.client.Watch(watchCtx, sh.key, clientv3.WithRev(resp.Header.Revision))

				deleted := false
				for !deleted {
					select {
					case <-sh.sess.DoneCh:
						watchCancel()
						return 0, fmt.Errorf("session lost while waiting for spread semaphore slot")
					case wresp, ok := <-watchChan:
						if !ok {
							deleted = true
							break
						}
						if wresp.Err() != nil {
							watchCancel()
							return 0, wresp.Err()
						}
						for _, ev := range wresp.Events {
							if ev.Type == clientv3.EventTypeDelete {
								deleted = true
								break
							}
						}
					}
				}
				watchCancel()

				// Check context done
				if ctx.Err() != nil {
					return 0, ctx.Err()
				}
				continue
			}
			// If already exists but not spread, we can overwrite or reuse it.
			// Since lease ID is unique, this shouldn't happen unless re-entering.
		} else {
			wroteKey = true
		}

		// Ensure we delete our key if we fail to acquire or time out
		defer func() {
			if wroteKey && !sh.acquired {
				// Clean up key
				delCtx, delCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer delCancel()
				_, _ = sh.client.Delete(delCtx, sh.key)
			}
		}()

		prefix := core.SemaPrefix(sh.name, sh.max)

		for {
			// 2. Fetch all keys under the prefix sorted by create-revision
			getResp, err := sh.client.Get(ctx, prefix,
				clientv3.WithPrefix(),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
			)
			if err != nil {
				return 0, err
			}

			// Find our rank
			rank := -1
			for i, kv := range getResp.Kvs {
				if string(kv.Key) == sh.key {
					rank = i
					break
				}
			}

			if rank == -1 {
				return 0, fmt.Errorf("our key was deleted from etcd")
			}

			// 3. If rank < N, we hold a slot!
			if rank < sh.max {
				sh.acquired = true
				return getResp.Kvs[rank].CreateRevision, nil
			}

			// Non-blocking means we don't wait if rank >= max
			if sh.waitLimit == 0 {
				return 0, context.DeadlineExceeded // Treat non-block as immediate timeout
			}

			// 4. Watch for deletion of the key at rank - max
			targetKey := string(getResp.Kvs[rank-sh.max].Key)

			// Watch from the revision of our Get response to avoid races
			watchCtx, watchCancel := context.WithCancel(ctx)
			watchChan := sh.client.Watch(watchCtx, targetKey,
				clientv3.WithRev(getResp.Header.Revision),
			)

			deleted := false
			for !deleted {
				select {
				case <-sh.sess.DoneCh:
					watchCancel()
					return 0, fmt.Errorf("session lost while waiting for semaphore slot")
				case wresp, ok := <-watchChan:
					if !ok {
						deleted = true
						break
					}
					if wresp.Err() != nil {
						watchCancel()
						return 0, wresp.Err()
					}
					for _, ev := range wresp.Events {
						if ev.Type == clientv3.EventTypeDelete {
							deleted = true
							break
						}
					}
				}
			}
			watchCancel()

			// Check context done
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
		}
	}
}

func (sh *SemaHolder) Release(ctx context.Context) error {
	if !sh.acquired {
		return nil
	}
	_, err := sh.client.Delete(ctx, sh.key)
	return err
}

func (sh *SemaHolder) Name() string {
	return sh.name
}

func (sh *SemaHolder) Key() string {
	return sh.key
}

func RunSema(ctx context.Context, logger *slog.Logger, endpoints []string, dialTimeout time.Duration, ttl time.Duration, killAfter time.Duration, name string, max int, spread bool, waitLimit time.Duration, cmdArgs []string) (int, error) {
	sess, err := core.NewCoreSession(ctx, endpoints, dialTimeout, ttl, logger)
	if err != nil {
		logger.Error("failed to create session", "err", err)
		return 69, err
	}
	defer sess.Close()

	val, err := core.NewHolderJSON(cmdArgs)
	if err != nil {
		return 64, err
	}

	holder := NewSemaHolder(sess, name, max, spread, string(val), waitLimit)

	exitCode, _, err := core.Run(ctx, logger, sess, holder, cmdArgs, killAfter)
	return exitCode, err
}

func CmdWho(ctx context.Context, client *clientv3.Client, name string, max int, useJSON bool) (int, error) {
	prefix := core.SemaPrefix(name, max)
	resp, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
	)
	if err != nil {
		return 1, err
	}

	if len(resp.Kvs) == 0 {
		return 1, nil // Empty, exit 1
	}

	for rank, kv := range resp.Kvs {
		state := "WAIT"
		if rank < max {
			state = "HELD"
		}

		var h core.HolderJSON
		if err := json.Unmarshal(kv.Value, &h); err != nil {
			h = core.HolderJSON{
				Host:    "unknown",
				Started: "unknown",
			}
		}

		if useJSON {
			outputJSON := map[string]interface{}{
				"name":    name,
				"max":     max,
				"state":   state,
				"host":    h.Host,
				"pid":     h.Pid,
				"started": h.Started,
				"rev":     kv.CreateRevision,
			}
			if h.Cmd != "" {
				outputJSON["cmd"] = h.Cmd
			}
			bytes, _ := json.Marshal(outputJSON)
			fmt.Println(string(bytes))
		} else {
			fmt.Printf("%s[%d/%d] %-4s  %s pid=%d since=%s rev=%d\n", name, max, max, state, h.Host, h.Pid, h.Started, kv.CreateRevision)
		}
	}

	return 0, nil
}
