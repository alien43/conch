# CONCH — a tiny etcd coordination suite

> *"You hold the conch, you may speak."* — Conch Orchestrates Nothing, Coordinates Hosts.

A suite of `flock(1)`-style tools in the spirit of LINK-is-not-keepalived: each does one
thing, each is a thin wrapper around the cluster's **existing etcd quorum** (the one Patroni
already depends on — all 3 nodes, plain HTTP on `:2379`). No new daemons of consequence, no
new state stores.

**Language:** Go, single binary `conch` with subcommands (busybox-style symlinks optional).
The `go.etcd.io/etcd/client/v3/concurrency` package already implements Session, Election and
Mutex; we add only the bounded-N semaphore recipe and CLI plumbing.

## 1. The unifying idea

Every tool holds something in etcd and runs a command *while holding it*:

```
conch <hold-a-thing> -- <command...>
```

When the hold is lost — lease expires, holder preempted, process exits — the wrapper kills
the child (SIGTERM, then SIGKILL after a grace period). There are only two holdable things,
both built on an etcd **lease** (a TTL the wrapper keepalives in the background):

| Primitive | Meaning | etcd mechanism |
| :--- | :--- | :--- |
| **election** | exactly one holder of a named "office" | `concurrency.Election` — campaign on a prefix, lowest create-revision wins |
| **semaphore** | at most N concurrent holders | leased keys under a prefix; you hold a slot iff your create-revision is among the N lowest, else watch and wait |

A mutex is a semaphore with N=1. `cron` is not a new primitive: it is a timer feeding a
per-fire-time mutex.

## 2. The tools

### 2.1 `conch elect` — leader election

```
conch elect <office> [--ttl 10s] [--restart] -- <cmd...>   # run cmd only while leader
conch elect <office> --who                                 # print current leader, exit
conch elect <office> --watch                               # stream leadership changes
```

* Campaigns for `<office>`; on winning, execs `<cmd>` with `CONCH_LEADER=1` and
  `CONCH_REV=<fencing token>` in the environment.
* On session loss or preemption: SIGTERM child, SIGKILL after `--kill-after` (default 5s).
* `--restart`: re-campaign and re-run after the child exits or leadership is lost
  (the "run this incus container on exactly one node" mode). Without it, exit when done.
* The campaign value is `host:pid:starttime`, so `--who` is actually informative.

Covers: *"asking who the current leader is, to display and debug"* and the future
*"run this incus container on exactly one node"*:

```
conch elect incus-frobnitz --restart -- incus start --console frobnitz
```

### 2.2 `conch sema` — counting semaphore / lock

```
conch sema <name> --max 3 [--ttl 10s] [--wait 30s|--nonblock] -- <cmd...>
conch sema <name> --who          # list current holders and waiters
```

* `--max 1` is the etcdctl-style lock.
* `--nonblock` exits with code **75** (EX_TEMPFAIL) if no slot is free — composable in
  shell, same idea as `flock -n`.
* `--max` is asserted by every client (it's in the key prefix: `sema/<name>/<max>/...`),
  so two nodes disagreeing on N can't oversubscribe — they'd simply be in different
  semaphores, which is loud and debuggable rather than silently wrong.

Covers: *"no more than 1, 2, 3 nodes can concurrently execute a given command"*:

```
conch sema juicefs-rebalance --max 2 -- juicefs gc ...
```

### 2.3 `conch cron` — dkron, the boring way

Two parts:

* **CLI** (anyone can schedule from anywhere):
  ```
  conch cron add <name> --schedule '*/15 * * * *' [--sema-max 1] -- <cmd...>
  conch cron ls [--last]        # jobs, schedules, last run + exit code + node
  conch cron rm <name>
  ```
* **`conch conchd`** — identical tiny daemon on every node (systemd unit via NixOS module).
  It watches the job set, and at each fire time **all nodes race for a mutex keyed on
  `(job, fire-time)`**. Winner runs the job and records the result; losers back off until
  the next tick. No scheduler node, no leader, nothing to fail over — the race *is* the
  scheduler.

Misfire policy: if all nodes were down across a fire time, the tick is skipped (cron
semantics, not queue semantics). Job results are written to
`cron/result/<name>/<fire-time>` with a TTL of a few days, so `cron ls --last` and
debugging work without a log pipeline.

## 3. Key schema

Everything under one prefix, human-readable with plain `etcdctl get --prefix`:

```
/conch/v1/elect/<office>/...              # managed by concurrency.Election
/conch/v1/sema/<name>/<max>/<lease-id>   # one leased key per holder/waiter
/conch/v1/cron/job/<name>                # job spec (JSON: schedule, cmd)
/conch/v1/cron/fire/<name>/<unix-ts>     # the per-tick claim (create-once race)
/conch/v1/cron/result/<name>/<unix-ts>   # exit code, node, duration (TTL ~3d)
```

(Schema details authoritative in `02_core.md` §6.)

## 4. Failure semantics — read this part

etcd gives **at-most-one-most-of-the-time, eventually-exactly-one** — not lock-step mutual
exclusion. The classic hole: a holder's network blips, its lease expires, etcd elects a
successor — but the old child process is still running for the few seconds until its
wrapper notices. Two nodes briefly overlap.

What CONCH promises and how:

1. **Fast kill on loss.** The wrapper treats keepalive failure as loss *immediately* (it
   does not wait for the TTL); child gets SIGTERM at most one keepalive interval (~TTL/3)
   after the partition starts. Overlap window ≈ TTL.
2. **Fencing token available.** `CONCH_REV` is the monotonic etcd revision of the win.
   Commands that talk to a resource which can check tokens (e.g. writes guarded by a
   Patroni-style check) should use it. Most won't — that's fine, see (3).
3. **Therefore: commands should be idempotent or crash-tolerant.** `incus start` is fine;
   a non-transactional migration is not. This is a documentation contract, not something
   the tool can enforce.

Defaults: `--ttl 10s` everywhere. Tighter TTL = faster failover and smaller overlap, but
more sensitivity to GC pauses / etcd hiccups. 10s is right for "incus container" workloads;
cron's per-tick mutex uses the job's expected runtime.

etcd unreachable ⇒ **fail closed**: a holder kills its child when it can't confirm the
lease; a candidate simply doesn't run. The suite inherits etcd's availability (loses writes
when 2 of 3 nodes are down — same blast radius as Patroni, which is the point).

## 5. Implementation notes

* **Deps:** `go.etcd.io/etcd/client/v3` (+ its `concurrency` package), `robfig/cron/v3`
  for schedule parsing, stdlib `flag` or `spf13/cobra` for CLI. Nothing else.
* **Endpoints:** `--endpoints` / `$CONCH_ENDPOINTS`, defaulting to
  `asus-server:2379,dell-server:2379,rpi-server:2379`.
* **Semaphore recipe** (the only non-library primitive, ~100 lines): put a leased key under
  the prefix in a txn; fetch all keys sorted by create-revision; if yours is in the lowest
  N you hold a slot; otherwise watch for deletion of the (rank−N)-th key ahead of you and
  re-check. Release = delete key / let lease lapse.
* **Exit codes:** child's exit code passed through; **75** = couldn't acquire
  (`--nonblock`/`--wait` exhausted); **70** = held but lost before child exited.
* **Packaging:** `buildGoModule` flake output; `nixos/modules/services/conch.nix` deploys
  `conchd` on all three nodes via `cluster-node.nix`. CLI goes into systemPackages.

## 6. Milestones

1. `conch elect` end-to-end (campaign, supervise, `--who`, `--watch`) — proves the
   session/supervision core every other tool reuses.
2. `conch sema` — adds the semaphore recipe on the same core.
3. `conch cron` + `conchd` NixOS module — composes 1+2 with a timer.
4. (later) `conch run` sugar for the incus case: `elect --restart` with per-resource
   conventions.

## 7. Endgame: replacing Swarm + Komodo

The long-term plan this suite serves: **retire Docker Swarm and Komodo in favour of Incus +
OCI containers + a tiny swarm-like scheduler.** CONCH's primitives are that scheduler's
substrate — no new mechanisms are needed, only composition:

| Swarm concept | CONCH composition |
| :--- | :--- |
| service, 1 replica | an election office per service: `conchd` watches `/conch/svc/<name>`, eligible nodes campaign, winner runs the incus container. Node death ⇒ lease lapses ⇒ another node wins ⇒ container starts there. Failover **is** election failover; there is no scheduler brain to make HA. |
| service, N replicas | spread semaphore: `--max N` with the added rule *each node holds at most one slot* (slot key includes hostname) ⇒ anti-affinity for free. |
| placement constraints | campaign eligibility: a node doesn't campaign for offices it doesn't match (labels, arch, storage). No bin-packing — for 3 nodes, filters are the right amount of scheduler. |
| rolling update | bump the spec revision; holders restart one-at-a-time gated by `sema <svc>-update --max 1`. |
| deploy (Komodo) | write the spec key to etcd, from anywhere. |
| overlay networking / ingress | **not replaced** — BGP VIPs + HAProxy already do this better, which is what makes Swarm removable at all. |

This stays out of scope for milestones 1–3, but it is why the supervision core (hold →
run → kill-on-loss) must be solid rather than convenient: the same loop that runs a cron
job will eventually run every service in the cluster.

## 8. Alternatives considered

* **dkron itself** — its own Raft/Serf cluster next to etcd; a second consensus system to
  operate. Rejected: we already pay for one.
* **Nomad** (see `nomad_exploration.md`) — solves the superset, but is a platform, not a
  tool. CONCH is deliberately the opposite end of that trade.
* **Shell over `etcdctl lock`/`elect`** — fastest prototype, but child supervision and
  lease-loss handling in shell are exactly where the correctness lives. Rejected.
* **Python** — lovely for prototyping, but unmaintained client libs, ~200ms interpreter
  startup on every invocation, and a heavy grpcio closure on NixOS. Rejected for the
  permanent suite.
