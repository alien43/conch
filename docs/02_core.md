# 02 — Shared core spec

Everything every tool inherits. This file is authoritative; `00_design.md` sketches.

## 1. Connection

| Flag | Env | Default |
| :--- | :--- | :--- |
| `--endpoints` | `CONCH_ENDPOINTS` | `localhost:2379` |
| `--dial-timeout` | `CONCH_DIAL_TIMEOUT` | `5s` |

Plain HTTP (matches the cluster's etcd; no TLS until etcd itself grows it). If no
endpoint is reachable within the dial timeout: exit **69** without side effects.

## 2. Session (lease)

| Flag | Env | Default |
| :--- | :--- | :--- |
| `--ttl` | `CONCH_TTL` | `10s` |

* One `concurrency.Session` per process; keepalive interval = TTL/3 (client default).
* **Loss detection:** `<-session.Done()` *or* a missed keepalive response — whichever
  fires first. We do not wait out the TTL: ambiguity is loss (principle 3).
* The session's lease ID and TTL are logged at acquisition.

## 3. Holder identity

The value stored at any held key is one-line JSON:

```json
{"host":"node2","pid":4242,"started":"2026-06-12T10:15:00Z","cmd":"my-daemon --port 8080"}
```

`cmd` is the child argv joined, truncated to 256 bytes; absent for pure CLI queries.

## 4. Supervision loop

The single code path used by `elect`, `sema`, and `conchd`:

1. Acquire hold (tool-specific). Record etcd **revision** of acquisition.
2. Start child in its **own process group** (`Setpgid`), with the inherited environment
   plus:

   | Var | Value |
   | :--- | :--- |
   | `CONCH_NAME` | office / semaphore / job name |
   | `CONCH_REV` | acquisition revision — the fencing token |
   | `CONCH_LEASE` | lease ID (hex) |
3. Wait on **either** child exit **or** loss signal:
   * Child exits first ⇒ release hold cleanly, exit with the child's code.
   * Loss first ⇒ SIGTERM the process group; after `--kill-after` (default `5s`)
     SIGKILL the group; reap; exit **70**.
4. SIGINT/SIGTERM to the wrapper: forward SIGTERM to the group, wait (same kill-after
   escalation), release hold, exit with the child's code.

No restart logic lives in the core; `--restart` loops *around* it (per-tool).

## 5. Exit codes (public API)

| Code | Meaning |
| :--- | :--- |
| child's code | child ran to completion while the hold was kept |
| **64** | usage error |
| **69** | etcd unreachable / no session could be established |
| **70** | hold lost while child was running (child was killed) |
| **75** | could not acquire: `--nonblock` set, or `--wait` exhausted |

Codes chosen from `sysexits.h`; 75 = `EX_TEMPFAIL` matches `flock -n` conventions in
spirit (retryable), 70 = `EX_SOFTWARE` (the run is tainted).

## 6. Key schema (public API, versioned)

All keys live under `/conch/v1/`:

```
/conch/v1/elect/<office>/...                # managed by concurrency.Election
/conch/v1/sema/<name>/<max>/<leaseID>       # one leased key per holder/waiter
/conch/v1/cron/job/<name>                   # job spec JSON
/conch/v1/cron/fire/<name>/<tick-unix>      # per-tick claim (create-once race)
/conch/v1/cron/result/<name>/<tick-unix>    # result JSON, TTL ~3 days
```

A breaking change to schema or semantics ⇒ `/conch/v2/`; tools never read across
versions. Debuggable by design: `etcdctl get --prefix /conch/v1/ ` tells you everything.

## 7. Logging

`log/slog`, text handler, stderr. One line per state transition:
`acquired`, `lost`, `child-start`, `child-exit`, `term-sent`, `kill-sent`, `released` —
each with `name`, `rev`, and timing. `--quiet` suppresses everything below WARN (for
cron-driven invocations). Nothing else is ever written to stderr in the happy path.
