# conch

[![Go Test Status](https://github.com/alien43/conch/actions/workflows/test.yml/badge.svg)](https://github.com/alien43/conch/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/alien43/conch/graph/badge.svg)](https://codecov.io/gh/alien43/conch)

Distributed coordination wrapper for Linux processes, built on etcd v3. Provides leader election, counting semaphores, and distributed cron without requiring application-level etcd integration.

## Install

Pre-built binaries (amd64, arm64) are available on the [releases page](https://github.com/alien43/conch/releases).

```bash
sudo curl -fsSL "https://github.com/alien43/conch/releases/latest/download/conch-$(uname -s | tr A-Z a-z)-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')" \
  -o /usr/local/bin/conch && sudo chmod +x /usr/local/bin/conch
```

NixOS: use `pkgs.conch` from the flake.

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
