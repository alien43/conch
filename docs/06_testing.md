# 06 — Testing model

Three layers, in increasing order of realism. All run from `nix flake check`; the dev
shell provides `go` and a real `etcd` binary — no mocks of etcd anywhere, ever. The
client-side recipes are exactly where bugs live; mocking etcd would test the mock.

## 1. Unit (pure logic only)

For code with no I/O: semaphore ranking ("is rank r < N; which key do I watch"),
key encode/decode, holder JSON, schedule → next-tick math (table-driven, includes DST
edges), backoff sequence. Plain `go test`, milliseconds.

## 2. Integration (real etcd per package)

`TestMain` execs a **single-node etcd** from `$PATH` into a `t.TempDir()` (unix socket
or random localhost port), tears it down after. Each test gets a unique key prefix so
tests parallelize within one etcd.

Per tool, the contract table from its spec is the test list. Non-obvious must-haves:

* **elect**: winner runs child with correct `CONCH_REV`; `Resign` on clean exit hands
  over immediately (< 1s, not TTL); `--who` against vacant office exits 1;
  `--restart` backoff resets after a 60s-stable child.
* **sema**: N+1 contenders ⇒ exactly N children running (poll `--who`); releasing one
  promotes exactly the next-by-revision waiter; mismatched `--max` lands in a separate
  prefix; `--nonblock` exits 75 fast; waiter timeout removes its queue key.
* **cron**: three conchd instances against one etcd, `@every 1s` job ⇒ each tick has
  exactly one result; killed winner ⇒ tick claimed, no result, **no rerun**; `rm`
  during run lets the run finish; restart doesn't claim past ticks.
* **core**: child gets SIGTERM then SIGKILL after kill-after when lease is revoked
  (use `etcdctl lease revoke` as the partition stand-in); wrapper exits 70; child
  process *group* dies (spawn a grandchild, assert it's gone).

## 3. Invariant tests (the sacred ones — principle 4)

Property-style tests for the only promise that matters: **bounded concurrency with
honest overlap accounting**.

Harness: K competing wrapper processes on one machine, each child appends
`(holder, start-ns, end-ns)` records to a shared file (O_APPEND). After a randomized
schedule of kills, lease revocations, and SIGSTOP/SIGCONT of wrappers (the GC-pause /
partition simulator), assert over the interval log:

1. At every instant, ≤ N intervals overlap **except** during a window of ≤ TTL after
   each induced loss event (the documented overlap window — we assert it, including
   that it *closes*).
2. After the last fault, the system converges: exactly the expected holders, all
   waiters either promoted or exited 75/70.
3. Fencing tokens strictly increase per office/slot across successions.

These run with `-race`, are seeded (`-run Invariant -seed=N` reproducible), and gate
any change to `internal/core`.

## 4. Cluster smoke (manual, scripted)

`conch-smoke.sh` in the repo, run after each deploy against the real 3-node cluster:

```
conch elect smoke-$$ --who            # expects vacant, exit 1
conch elect smoke-$$ -- true          # acquire + clean exit on real quorum
conch sema  smoke-$$ --max 2 --who
conch cron  add smoke-$$ --schedule '@every 1m' -- logger "conch smoke"
# ... one tick later: cron ls shows exactly one result; then cron rm
```

Plus the two drills worth doing by hand once per schema version: power off the
fire-key-winning node mid-run (expect: no rerun, visible zombie row in `ls`), and stop
etcd on 2 of 3 nodes while an `elect --restart` service runs (expect: child killed ≤
TTL, restarts when quorum returns).

## CI shape

`nix flake check` = gofmt + go vet + layers 1–3. Layer 3 with a fixed seed in CI and a
randomized seed in a nightly `conch cron` job on the cluster itself — the suite
eventually tests itself, which is either elegant or hubris, and we'll find out which.
