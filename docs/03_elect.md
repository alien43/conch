# 03 — `conch elect`

Run a command only while holding a named office; observe elections.

## Synopsis

```
conch elect <office> [--ttl 10s] [--restart] [--kill-after 5s] [--on-acquire CMD] [--on-lose CMD] [--hook-timeout 30s] [--fence CMD --fence-budget D] [--watchdog [--watchdog-heartbeat PATH --watchdog-stale D]] -- <cmd...>
conch elect <office> --who [--json]
conch elect <office> --watch [--json]
conch elect <office> --assert [--min-rev N] [--json]
```

## Behavior

### Run mode (`-- <cmd>`)

1. Open session; `Election.Campaign(ctx, holderJSON)` on
   `/conch/v1/elect/<office>/`. Campaign blocks until won (use `--wait`/`--nonblock`
   from the core flags to bound it).
2. On winning:
   * If `--on-acquire CMD` is configured, execute the hook. If the hook fails (non-zero exit or timeout), resign from the election, back off (if `--restart` is set), and do not start the child.
   * If `--on-acquire` succeeds (or isn't set), enter the supervision loop (`02_core.md` §4). `CONCH_REV` = the election's create-revision — this is the fencing token; it increases monotonically with each new leader of the office.
3. On loss: child killed.
   * If `--on-lose CMD` is configured, run the teardown/cleanup hook (best-effort; failures do not block restart).
   * After hooks/termination complete:
     * without `--restart`: exit 70.
     * with `--restart`: log, **resign** (drop any stale candidacy), back off
       (exponential, 1s → 30s cap, decorrelated jitter), re-campaign, re-run. Also
       re-runs after a *clean* child exit. `--restart` mode never exits on its own;
       it is stopped by signal. Backoff resets after a child survives 60s.
4. Clean child exit (no `--restart`):
   * Run `--on-lose CMD` hook if configured.
   * `Resign()` so the next candidate wins immediately rather than waiting for TTL, then exit with the child's code.

### Transition Hooks (`--on-acquire` / `--on-lose`)

Transition hooks allow executing setup/teardown commands synchronously on leadership transition edges:
* **`--on-acquire CMD`**: Runs after winning the office, but *before* the child process is started. Inherits the child's environment variables (`CONCH_NAME`, `CONCH_REV`, `CONCH_LEASE`). A non-zero exit code halts execution and is treated as a failed campaign (exit 75). The hook is supervised by the lease like the child: if the lease is lost while it runs, its process group is killed at once, the child is never started, and the wrapper exits 70 (or re-campaigns under `--restart`). The lease is re-checked once more right before the child starts.
* **`--on-lose CMD`**: Runs after the child has been fully killed/terminated (e.g., after `SIGTERM` -> `SIGKILL` escalation completes). Runs before the wrapper re-campaigns (under `--restart`) or exits. It runs whenever `--on-acquire` *started* or the child started — including when `--on-acquire` failed, was killed by lease loss, or the lease was lost before the child could start — so a promote is always paired with a demote. This is a best-effort cleanup; failures are logged but do not halt progress.
* **Timeout & Isolation**: Both hooks run in the office holder's process group with group-level isolation and are bounded by `--hook-timeout` (default `30s`). If a hook times out, its process group is killed and the execution is treated as a failure.

### Fencing (`--fence CMD --fence-budget D`)

Killing the child's process group is enough when the child *is* the work. It is not
enough when the child started something outside its process group: a container, a VM,
a process under another supervisor. `--fence` is the command that stops that work, with
a hard deadline that conch guarantees fits inside the lease.

* **When it runs:** on every end of a term in which `--on-acquire` or the child started:
  lease lost, the guardian deadline, child exit, a signal, cancellation. It also runs when
  the lease is lost before the child could start.
* **Who starts it:** conch's guardian, on conch's own clock. It does not wait for the child,
  so a wedged child (SIGSTOP, a hung syscall) cannot delay it. On lease loss it runs
  **alongside** the child's SIGTERM → kill-after → SIGKILL, not after it.
* **Deadline:** the fence runs in its own process group. At `--fence-budget` the group is
  SIGKILLed and the fence has **failed**. Only exit 0 within the budget confirms.
* **Before resign:** on a graceful end (child exit, signal) the fence confirms *before* the
  office is resigned, so no rival can win while the work may still run.
* **Timing:** the fence starts at loss detection, `detect` after the newest successful
  renewal was *sent* (`02_core.md` §2). The guardian also starts it at the latest when
  `ValidUntil` leaves `budget + 1s`. etcd cannot expire the lease before `ttl` after that
  send. At startup conch refuses (exit **64**) unless

  ```
  detect + fence-budget + 1s < ttl
  ```

  (`core.FitFence`; 3s fits at the default TTL 10s, 13s at TTL 30s).
* **Environment:** `CONCH_NAME`, `CONCH_REV`, `CONCH_LEASE`, and `CONCH_FENCE_REASON`, one of
  `lost`, `lease-deadline`, `child-exit`, `signal`, `cancelled`, `retry`.
* **A failed fence** (non-zero exit, or killed at the budget):
  - conch does **not** resign and does **not** revoke the lease. The office stays held until
    the lease expires on its own, one TTL after the last renewal.
  - Without `--restart`, conch exits **71**.
  - With `--restart`, conch retries the fence with backoff (1s → 30s,
    `CONCH_FENCE_REASON=retry`) and campaigns again only after a retry confirms.
  - A fence that failed while conch was being stopped (signal or cancellation) is not
    retried: exit 71.

| | `--on-lose` | `--fence` |
| :--- | :--- | :--- |
| purpose | cleanup / demote | stop the work before a rival can start it |
| when | after the child is fully dead | at loss detection, alongside the kill; before resign |
| bound | `--hook-timeout` | `--fence-budget`, validated against the TTL |
| failure | logged, ignored | exit 71, no resign, no re-campaign until a retry confirms |

Measured (`internal/elect/fence_test.go`, TTL 6s, budget 1s, real etcd): on a partition the
fence started 2.9–3.0s before the office key vanished server-side, with or without a
SIGSTOPped child.

### Watchdog (`--watchdog [--watchdog-heartbeat PATH --watchdog-stale D]`)

Under systemd with `WatchdogSec=`, conch pets the watchdog (`sd_notify WATCHDOG=1`, every
`WATCHDOG_USEC/2`) **only while it is fit**. systemd then acts on a conch that can no longer
guarantee its fence: it kills and restarts conch, or reboots the host if the unit sets
`FailureAction=reboot-force`.

| State | Fit when |
| :--- | :--- |
| not holding, nothing left unfenced | always |
| holding | the lease bound still leaves `fence-budget + 1s` (the fence's start deadline has not passed), **and**, with `--watchdog-heartbeat`, the child touched the file within `--watchdog-stale` (a grace of one stale period from the term's start) |
| fencing | the fence is still within its budget |
| a fence failed | never, until a retry confirms |

conch sends `READY=1` once at start, so `Type=notify` works.

* **Child health goes through the heartbeat file, not `NotifyAccess=all`.** systemd keeps one
  watchdog timer per unit, and *any* accepted `WATCHDOG=1` resets it. If both conch and the
  child petted, a live conch would hide a wedged child and the other way round. Keep the
  default `NotifyAccess=main`, and have the child touch the heartbeat file from its main loop.
* Without `WATCHDOG_USEC`/`NOTIFY_SOCKET` (no `WatchdogSec=`), or when `WATCHDOG_PID` names
  another process, `--watchdog` logs `watchdog-disabled` and conch runs without it.
* conch never opens `/dev/watchdog*`.

### `--who`

Prints the current leader's holder JSON (or, without `--json`, a single line
`<office> <host> pid=<pid> since=<started> rev=<rev>`). No leader ⇒ prints nothing,
exits **1**. Read-only: no lease is created.

### `--watch`

Streams one line per leadership change (same format as `--who`), starting with the
current state. An office becoming vacant emits `<office> -`. Runs until signalled.

### `--assert` (Predicate Check)

A read-only predicate to safely check if the current host holds leadership of an office:
* **Exit 0** if this host currently holds the office (and, if `--min-rev N` is given, the office's create-revision is $\ge N$).
* **Exit 1** if the office is vacant, held by another host, or if its create-revision is $< N$.
* **Exit 69** if etcd is unreachable (fail-closed behavior).
* **`--json` option**: Prints `{"held":true,"rev":1234,"host":"hostname"}` to stdout.

## Flags

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--restart` | off | re-campaign and re-run forever (service mode) |
| `--kill-after` | `5s` | SIGTERM → SIGKILL escalation delay; the default is lowered to fit the TTL (`02_core.md` §2.1) |
| `--wait` | infinite | max time to campaign before giving up (exit 75) |
| `--nonblock` | off | equivalent to `--wait 0` |
| `--assert` | off | assert if this host holds the office (exit 0/1/69) |
| `--min-rev` | `0` | minimum create-revision for the assert predicate check |
| `--on-acquire` | empty | command to run after winning, before child starts |
| `--on-lose` | empty | command to run after child is killed, before re-campaigning/exit |
| `--hook-timeout` | `30s` | timeout duration for transition hooks |
| `--fence` | empty | command that stops the work the child started; must exit 0 within `--fence-budget` (§ Fencing) |
| `--fence-budget` | — | hard deadline for `--fence`; required with it; must fit the TTL |
| `--watchdog` | off | pet systemd's watchdog only while fit (§ Watchdog) |
| `--watchdog-heartbeat` | — | file the child touches; stale while holding = unfit |
| `--watchdog-stale` | — | max heartbeat age; required with `--watchdog-heartbeat` |

Plus core flags (`--endpoints`, `--ttl`, `--quiet`, `--json`).

## Failure behavior

* Overlap window on partition ≈ TTL (00_design §4): the deposed leader's child dies
  within one keepalive interval of the wrapper noticing; a successor may have already
  started. Commands must tolerate this or check `CONCH_REV`.
* etcd quorum loss while leading ⇒ treated as loss (fail closed) ⇒ child killed even
  though no rival can be elected. This is deliberate: we can't *prove* we still hold.

## Examples

```sh
# run a daemon on exactly one node at a time (active-passive service)
conch elect my-daemon --restart -- /usr/local/bin/my-daemon

# run hooks around leadership transitions (e.g. promoting databases)
conch elect db-primary --on-acquire "pg_ctl promote" --on-lose "pg_ctl demote" --restart -- postgres

# debug: who currently holds the leader-office?
conch elect leader-office --who

# check if this host is the active leader with a minimum term of 100
if conch elect leader-office --assert --min-rev 100; then
  echo "I am the active primary"
fi

# dashboard: watch all leadership churn for an office
conch elect leader-office --watch
```
