# 05 — `conch cron` / `conch conchd`

Distributed cron: a schedule executed by exactly one node per tick. Anyone schedules.

## Synopsis

```
conch cron add <name> --schedule '<cron expr>' [--run-ttl 10m] [--quiet] -- <cmd...>
conch cron ls [--last] [--json]
conch cron rm <name>
conch conchd            # the per-node daemon (systemd unit; identical on every node)
```

## Job spec

`conch cron add` writes JSON to `/conch/v1/cron/job/<name>` (no lease — jobs persist):

```json
{
  "schedule": "*/15 * * * *",
  "cmd": ["restic", "backup", "/mnt/juicefs"],
  "run_ttl": "10m",
  "added_by": "andrey@asus-server",
  "added_at": "2026-06-12T10:15:00Z"
}
```

* `schedule`: standard 5-field cron, parsed by `robfig/cron/v3` (also accepts
  `@hourly`, `@every 4h`). Validated at `add` time; invalid spec never enters etcd.
* `run_ttl`: expected max runtime; bounds the job's etcd session TTL (the lease behind
  the supervision loop stays the core default; `run_ttl` only caps how long conchd will
  let the child run before declaring it hung and killing it). Default `10m`.
* `add` on an existing name overwrites (it's how you edit). `rm` deletes; a run already
  in flight finishes.

## How a tick runs (no scheduler node)

Every node runs an identical `conchd`:

1. **Watch** `/conch/v1/cron/job/` (plus initial list) ⇒ in-memory job table, kept
   current within one watch event.
2. For each job, compute the next tick `T` from the *schedule* (tick identity is the
   scheduled time, never `time.Now()` — clock skew between chrony-synced nodes moves
   *when* a node tries, not *which tick* it claims).
3. At `T` (+ up to 500ms random jitter, politeness not correctness), race:
   `Txn If(CreateRevision(fire/<name>/<T>) == 0) Then(Put holderJSON, TTL 25h)`.
   * **Win** ⇒ run the job through the standard supervision loop (`02_core.md` §4),
     under the conchd session. `CONCH_NAME=<name>`, `CONCH_REV` = txn revision.
   * **Lose** ⇒ do nothing until the next tick. Losing is the normal case ×(N−1).
4. On child exit, write `/conch/v1/cron/result/<name>/<T>` (TTL 3 days):

   ```json
   {"node":"rpi-server","exit":0,"started":"...","duration":"4.2s"}
   ```

### Semantics: at-most-once per tick

The fire key has a **fixed 25h TTL, not the session lease**. If the winning node dies
mid-run, the claim survives, so no other node re-runs that tick — *at-most-once*,
matching cron's nature (a skipped run is recovered by the next tick, not by replay).
A tick where every node was down is skipped entirely (misfire policy: skip, never
queue). The result key's absence next to a claimed fire key is the "died mid-run"
signature — visible in `cron ls --last`.

## `cron ls`

```
NAME            SCHEDULE      LAST-TICK             NODE         EXIT  DURATION
restic-juicefs  */15 * * * *  2026-06-12T10:45:00Z  rpi-server   0     4.2s
certs-renew     @daily        2026-06-12T00:00:00Z  dell-server  0     11s
zombie-job      @hourly       2026-06-12T10:00:00Z  asus-server  ?     —      ← claimed, no result
```

`--last` adds the last result's stderr tail? **No** — results carry no output. Job
output goes to the journal of the node that ran it (conchd logs child stdout/stderr via
slog). Keeping output out of etcd is deliberate: etcd is a coordination store, not a
log pipeline.

## Failure behavior

* **Clock skew**: a fast node claims tick `T` early by (skew − jitter); a slow node
  simply loses the race. Correctness holds for skew up to min-interval/2; chrony keeps
  us orders of magnitude inside that.
* **conchd restart** (deploy): on startup, conchd does *not* claim ticks in the past —
  only ticks ≥ startup time. Combined with at-most-once, a rolling conchd deploy can at
  worst skip ticks that fire during the restart window of *all three* nodes at once
  (i.e., effectively never).
* **etcd down at tick time**: nobody can claim ⇒ tick skipped. Fail closed.

## Examples

```sh
conch cron add restic-juicefs --schedule '*/15 * * * *' -- restic backup /mnt/juicefs
conch cron add certs-renew --schedule '@daily' --run-ttl 5m -- ./renew.sh
conch cron ls --last
conch cron rm certs-renew
```
