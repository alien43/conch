# 08 — `conch-vm`: run a VM on one of two machines, no shared storage

The first concrete form of the §7 endgame (00_design): run an Incus VM (or OCI
container) on exactly one of two standalone Incus hosts, with failover, **without**
Incus clustering and its shared-storage (ceph) requirement.

## 1. The split Incus refuses to make

Incus clustering couples *who runs it* with *where the disk bytes live* — failover
only works on shared storage. We separate them:

| Problem | Solved by |
| :--- | :--- |
| who runs it | `conch elect` (03_elect) — exactly as already specified |
| where the bytes live | leader→follower replication: `incus copy --refresh` over btrfs/zfs incremental send |

This is the `ha_stack.md` Golden Rule applied to VMs: the running VM gets **raw local
NVMe**; HA comes from replication + election, not a distributed filesystem underneath.

## 2. Topology

* Two **standalone** incus daemons (no incus clustering), each added as an
  authenticated remote of the other (`incus remote add <peer> --token ...`).
* `boot.autostart=false` on every managed instance, both copies, always.
  **conch is the only thing that ever starts a managed instance** — that is the
  fencing mechanism; a node that lost its lease cannot have an auto-started zombie.
* etcd quorum stays 3-node (the witness participates in elections, never runs VMs —
  it simply has no local copy and never campaigns, see eligibility below).

## 3. The three verbs

`conch-vm` is a composition script (shell or a later `conch vm` subcommand — it adds
no new primitives, per 01_principles §1).

### `conch-vm run <name>` — the elect child

Deployed as: `conch elect vm-<name> --restart -- conch-vm-run <name>`
(a systemd unit per VM, enabled on both candidate hosts).

1. **Eligibility gate** (before the wrapper campaigns, as a pre-check loop): refuse
   to campaign unless (a) a local copy of `<name>` exists, and (b) the pin key
   (§5) is absent or names this host. Re-check on a timer; eligibility is dynamic.
2. On win: `incus start <name>`; then wait.
3. On SIGTERM (lease lost / drain): `incus stop <name>` (graceful, guest shutdown);
   after `--kill-after`-style grace, `incus stop --force`. Exit.

The runner never creates, deletes, or copies instances. It starts and stops. Only.

### `conch-vm sync <name>` — replication, leader→follower

Deployed as: `conch cron add vm-<name>-sync --schedule '@every 10m' -- conch-vm-sync <name>`

1. Exit 0 unless this host currently holds `vm-<name>` (check `elect --who`).
   The cron race picks an arbitrary node per tick; the guard makes only the
   leader's execution meaningful. Non-leaders no-op.
2. `incus snapshot create <name> repl-<tick>` — crash-consistent point.
3. `incus copy <name> <peer>:<name> --refresh` — incremental snapshot delta via
   btrfs/zfs send. Prune old `repl-*` snapshots beyond the last 3, both sides.

**RPO = sync interval.** A crash failover boots a copy at most one interval old.
Pick the interval per VM by how much history it can afford to lose.

### `conch-vm move <name> <host>` — planned move

1. Write pin key `/conch/v1/pin/vm-<name> = <host>`.
2. Current holder's eligibility gate sees the pin: runner does a **final sync**
   (stop guest → one last `copy --refresh`, seconds of delta) and resigns.
3. `<host>` is now the only eligible campaigner; it wins and starts.

Downtime = guest stop + final delta + start. This pin mechanism is the placement /
campaign-eligibility machinery from 00_design §7, exercised on its simplest case.

## 4. Why this is sound: replication follows leadership

After a crash failover, the new leader runs on a slightly stale snapshot while the
dead node holds newer-but-orphaned bytes. When the old node returns it does **not**
fight: it isn't leader, so its sync no-ops; the new leader's sync now refreshes in
the opposite direction, overwriting the orphaned divergence. The cluster self-heals
to the leader's timeline *by construction* — one source of truth at a time, chosen by
the election, no reconciliation logic anywhere.

The cost, stated honestly (per 01_principles §6): on crash failover, up to one sync
interval of writes is **discarded forever** once the new leader's first sync runs.
This is the deliberate trade against shared storage. VMs that cannot afford it keep
their precious state *outside* the VM disk — in the HA Postgres, Valkey, or JuiceFS
this cluster already provides — and treat the disk as rebuildable. (No DRBD; a
synchronous-replication tier is explicitly out of scope.)

## 5. Keys

```
/conch/v1/elect/vm-<name>/...        # the office (standard elect)
/conch/v1/pin/vm-<name>              # value: hostname; absent = run anywhere eligible
```

The pin key is plain (no lease) — pins persist. `conch-vm move` is the only writer;
deleting the pin returns the VM to free placement.

## 6. Failure matrix

| Event | Outcome |
| :--- | :--- |
| Holder crashes | peer wins ≤ TTL later, boots snapshot ≤ 1 sync interval old |
| Holder partitioned from etcd | runner stops VM (fail closed, ≤ TTL); peer wins and boots — brief overlap possible per 00_design §4, but the partitioned side is *stopping* while the new side starts, and only one side's writes survive the next sync |
| Both incus hosts down | nothing runs; first eligible host back wins (witness can't — no local copy) |
| Sync fails repeatedly | leadership unaffected; staleness grows — alert via the zombie/heartbeat pattern (07_deployment §5): `cron ls` shows failing `vm-*-sync` exits |
| Peer dead during `move` | pin written but target can't campaign; VM is down until target returns or pin is deleted (move is an explicit operator action; no automatic fallback) |

## 7. Milestone placement

After cron (milestone 3). Order within: runner + manual sync first (failover works,
moves are manual pin edits), `conch-vm move` sugar second. OCI containers under
incus use the identical pattern — only the instance type differs.
