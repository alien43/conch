# 03 — `conch elect`

Run a command only while holding a named office; observe elections.

## Synopsis

```
conch elect <office> [--ttl 10s] [--restart] [--kill-after 5s] [--on-acquire CMD] [--on-lose CMD] [--hook-timeout 30s] -- <cmd...>
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
* **`--on-acquire CMD`**: Runs after winning the office, but *before* the child process is started. Inherits the child's environment variables (`CONCH_NAME`, `CONCH_REV`, `CONCH_LEASE`). A non-zero exit code halts execution and is treated as a failed campaign.
* **`--on-lose CMD`**: Runs after the child has been fully killed/terminated (e.g., after `SIGTERM` -> `SIGKILL` escalation completes). Runs before the wrapper re-campaigns (under `--restart`) or exits. This is a best-effort cleanup; failures are logged but do not halt progress.
* **Timeout & Isolation**: Both hooks run in the office holder's process group with group-level isolation and are bounded by `--hook-timeout` (default `30s`). If a hook times out, its process group is killed and the execution is treated as a failure.

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
| `--kill-after` | `5s` | SIGTERM → SIGKILL escalation delay |
| `--wait` | infinite | max time to campaign before giving up (exit 75) |
| `--nonblock` | off | equivalent to `--wait 0` |
| `--assert` | off | assert if this host holds the office (exit 0/1/69) |
| `--min-rev` | `0` | minimum create-revision for the assert predicate check |
| `--on-acquire` | empty | command to run after winning, before child starts |
| `--on-lose` | empty | command to run after child is killed, before re-campaigning/exit |
| `--hook-timeout` | `30s` | timeout duration for transition hooks |

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
