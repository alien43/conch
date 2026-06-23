# conch

[![Go Test Status](https://github.com/alien43/conch/actions/workflows/test.yml/badge.svg)](https://github.com/alien43/conch/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/alien43/conch/graph/badge.svg)](https://codecov.io/gh/alien43/conch)

## What is Conch?

Distributed process supervisor and coordination wrapper (leader election, semaphores, cron) built on etcd v3 for zero-dependency Linux process orchestration.

Conch provides distributed coordination wrappers for Linux processes without requiring application-level etcd integration.

## How it works (Architecture)

Conch acts as a supervisor wrapper around your child processes. Under the hood, it interacts with your existing etcd v3 quorum:
* **Leases & Keepalives**: Conch campaigns for leadership or semaphores by creating keys bound to etcd leases. It runs a background keepalive loop to maintain the hold. If etcd quorum is lost or the network partitions, Conch detects the keepalive failure immediately and terminates the child process group (SIGTERM followed by SIGKILL) to enforce fail-closed behavior.
* **Watches**: For queuing primitives (like semaphores) and distributed scheduler updates (like cron job additions or deletions), Conch establishes etcd watches to respond immediately to cluster state transitions, minimizing wake-up delays and scheduling overhead.
* **Fencing Tokens**: Upon winning an election or slot, Conch exports the etcd create-revision as `CONCH_REV` to the child's environment, allowing downstream resources to perform optimistic concurrency checks.

This architecture ensures zero-dependency orchestration—your applications do not need to import etcd SDKs or implement consensus handling themselves.

## Quick Start

### Install

Install the latest version via Go:
```bash
go install github.com/alien43/conch/cmd/conch@latest
```

Or download pre-built binaries:
```bash
sudo curl -fsSL "https://github.com/alien43/conch/releases/latest/download/conch-$(uname -s | tr A-Z a-z)-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')" \
  -o /usr/local/bin/conch && sudo chmod +x /usr/local/bin/conch
```

### Try it out

```bash
# Campaign for a leader office
conch elect my-office -- echo "I am the leader!"

# Limit concurrency to 2 slots cluster-wide
conch sema heavy-jobs --max 2 -- sleep 5
```

## Build

Using [just](https://github.com/casey/just):
```bash
just build
```

Or manually:
```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o conch cmd/conch/main.go
```

## Global options

```
CONCH_ENDPOINTS     etcd endpoints, comma-separated (default: localhost:2379)
CONCH_DIAL_TIMEOUT  dial timeout (default: 5s)
CONCH_TTL           lease TTL (default: 10s)
```

Can also be passed as flags.

Other global flags:
- `--quiet`: suppress log output below `WARN` level.

---

## elect

Run a command under a cluster-wide leader lease. Blocks until the office is vacant, then spawns the command. If the lease is lost, the child process is killed.

```bash
conch elect <office> [flags] -- <cmd...>
```

| Flag | Default | Description |
|---|---|---|
| `--restart` | off | re-campaign after child exits |
| `--kill-after` | 5s | grace period before SIGKILL |
| `--wait` | forever | give up after this duration |
| `--nonblock` | off | exit 75 immediately if office is held |
| `--on-acquire` | — | shell command run on winning the office |
| `--on-lose` | — | shell command run on losing the office |
| `--hook-timeout` | 30s | timeout for hook commands |
| `--json` | off | print output as JSON (for `--who`, `--watch`, `--assert`) |
| `--min-rev` | 0 | minimum create-revision for `--assert` |

```bash
# Run a daemon, restart on failure
conch elect api-leader --restart -- /usr/local/bin/api-server

# Promote/demote postgres on leadership change
conch elect db-primary --on-acquire "pg_ctl promote" --on-lose "pg_ctl demote" --restart -- postgres

# Check who holds the office
conch elect api-leader --who
conch elect api-leader --who --json

# Watch leadership changes
conch elect api-leader --watch
conch elect api-leader --watch --json

# Assert this host holds the office (exit 0 yes, 1 no, 69 etcd unreachable)
conch elect api-leader --assert
conch elect api-leader --assert --min-rev 100 --json
```

---

## sema

Limit concurrent execution to N slots across the cluster.

```bash
conch sema <name> --max N [flags] -- <cmd...>
```

| Flag | Default | Description |
|---|---|---|
| `--max` | required | max concurrent holders |
| `--spread` | off | at most 1 slot per hostname |
| `--wait` | forever | give up after this duration |
| `--nonblock` | off | exit 75 immediately if no slot available |
| `--kill-after` | 5s | grace period before SIGKILL |
| `--json` | off | print output as JSON (for `--who`) |

```bash
# At most 3 nodes run this at once
conch sema heavy-jobs --max 3 -- /usr/local/bin/process-video.sh

# One slot per host, 5 total
conch sema ingest --max 5 --spread -- /usr/local/bin/ingest

# Check who holds slots
conch sema heavy-jobs --max 3 --who
conch sema heavy-jobs --max 3 --who --json
```

---

## cron

Register and manage distributed cron jobs. Each tick runs exactly once across the cluster.

```bash
# Register a job
conch cron add <name> --schedule '<expr>' [--run-ttl 10m] -- <cmd...>

# Remove a job
conch cron rm <name>

# List jobs
conch cron ls [--last] [--json]
```

```bash
# Run conchd on each cluster node to execute scheduled jobs
conch conchd

# Run conchd status HTTP API server on port 9191 (can also use CONCH_STATUS_ADDR env var)
conch conchd --status-addr :9191
```

---

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1–63 | child exit code (passed through) |
| 64 | usage error |
| 69 | etcd unreachable at startup |
| 70 | lease lost during run; child was killed |
| 75 | `--nonblock` or `--wait` expired |

## Why Conch? (Comparison to Alternatives)

Conch provides distributed coordination primitives (`elect`, `sema`, `cron`) as a simple process wrapper. Here is how it compares to other common solutions:

| Feature / Tool | **Conch** | **flock** | **etcdctl lock** | **Nomad / K8s** | **dkron** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Scope** | Cluster-wide | Local machine | Cluster-wide | Cluster-wide | Cluster-wide |
| **Child Supervision** | Yes (kills child on lease loss) | Yes (local OS lock) | No (wrapper script must handle loss) | Yes (full platform agent) | Yes (cron only) |
| **Decentralized** | Yes (etcd quorum) | Yes (local kernel) | Yes (etcd quorum) | No (needs controller nodes) | No (needs dkron nodes) |
| **Dependencies** | etcd only | None | etcd only | Heavy platform | dkron cluster |
| **Fencing Tokens** | Yes (`CONCH_REV`) | No | No | Yes (some engines) | No |

### Conch vs. `flock`
* `flock` is file-based and limited to a single host. `conch` works cluster-wide using etcd consensus. However, `conch` respects the same usage conventions (e.g. `--nonblock` returns exit code `75`, similar to `flock -n`).

### Conch vs. `etcdctl lock` / `consul lock`
* Standard CLI lockers like `etcdctl lock` only acquire the lock, then run the command without active lease-loss monitoring. If the network partitions and the lease expires in etcd, the runner is not notified, resulting in two nodes running the same exclusive resource (split-brain).
* `conch` actively keepalives the lease in the background. If the lease is lost or etcd becomes unreachable, `conch` immediately escalates termination (`SIGTERM` followed by `SIGKILL`) to guarantee fail-closed behavior.

### Conch vs. Kubernetes / Nomad / dkron
* Full-fledged orchestrators require heavy control planes, master nodes, database backends, and agents running on every machine.
* `conch` is a single static Go binary. If you already have an etcd cluster (which most Kubernetes/Consul/control-plane architectures do), `conch` leverages it directly with zero additional server overhead.

---

## Test

Using [just](https://github.com/casey/just):
```bash
just test
just test-chaos
```

Or manually:
```bash
go test -v -race ./...
./test-cluster-chaos.sh
```

---

## Docs

- [docs/00_design.md](docs/00_design.md) — motivation and primitives
- [docs/01_principles.md](docs/01_principles.md) — coding principles
- [docs/02_core.md](docs/02_core.md) — process supervision and lease spec
- [docs/03_elect.md](docs/03_elect.md) — leader election spec
- [docs/04_sema.md](docs/04_sema.md) — semaphore spec
- [docs/05_cron.md](docs/05_cron.md) — distributed cron spec
- [docs/06_testing.md](docs/06_testing.md) — testing model
- [docs/07_deployment.md](docs/07_deployment.md) — deployment and rollout spec
