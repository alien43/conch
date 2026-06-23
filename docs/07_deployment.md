# 07 — Deployment & Rollout

Conch is distributed as a single statically-compiled Go binary, requiring no external dependencies, dynamic loaders, or library runtimes. This makes deployment straightforward across diverse Linux distributions.

## 1. Binary Distribution

For production deployments, compile a statically-linked binary for the target architecture:

```sh
# Build statically-linked amd64 binary
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o conch ./cmd/conch
```

This binary has zero dynamic dependencies and can be safely copied to any compatible target host.

## 2. Daemon Supervision (systemd)

To run the distributed cron runner (`conch conchd`), it should be supervised by a process manager like systemd.

A standard systemd service unit template (`/etc/systemd/system/conchd.service`):

```ini
[Unit]
Description=Conch Distributed Cron Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment="CONCH_ENDPOINTS=http://127.0.0.1:2379"
ExecStart=/usr/local/bin/conch conchd
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Configure `CONCH_ENDPOINTS` to point to your cluster's etcd nodes.

## 3. Configuration Management & Declarative Jobs

While cron jobs are stored inside etcd, you can manage their lifecycle declaratively using configuration management tools (Ansible, Chef, SaltStack, or NixOS).

An automation script or module should deploy jobs by executing:

```sh
# Add or overwrite a job in the cluster
conch cron add <job-name> --schedule '<expr>' -- <cmd...>
```

Since `conch cron add` is idempotent, running it repeatedly will simply update the job specification in etcd.

## 4. Rollout & Upgrade Strategy

When deploying upgrades to the `conch` binary or `conchd`:

1. **Deploy Binary**: Copy the updated `conch` binary to `/usr/local/bin/conch` on all nodes. (This is a non-destructive binary delivery).
2. **Verify CLI**: Run a test command to verify connection to the etcd cluster:
   ```sh
   conch elect rollout-test --who
   ```
3. **Restart Daemon**: Perform a rolling restart of the `conchd` services on each node:
   ```sh
   systemctl restart conchd
   ```
   *Note: Because of lease state and tick intervals, rolling restarts do not skip execution ticks unless all nodes are offline concurrently for longer than the tick window.*

## 5. Observability & Monitoring

* **Logs**: Standard output and error from both `conchd` and the child processes are written directly to stdout/stderr. If using systemd, these are captured by journald:
  ```sh
  journalctl -u conchd.service -f
  ```
* **Liveness Probes**: Check that the host can campaign and talk to etcd by running an assertion check:
  ```sh
  conch elect monitor-probe --assert
  ```
* **Status HTTP API**: If `conchd` has `--status-addr` enabled, query status or health directly via HTTP:
  ```sh
  # Query cron, election, or semaphore statuses
  curl -fsS http://localhost:9191/cron
  curl -fsS http://localhost:9191/elect
  curl -fsS http://localhost:9191/sema
  ```
* **Dead Man's Snitch**: Register a simple heartbeat cron job to ping an external monitoring endpoint:
  ```sh
  conch cron add heartbeat --schedule '@every 5m' -- curl -fsS https://hchk.io/your-uuid
  ```
  If the endpoint stops receiving pings, it indicates a failure of the cron runner daemon, loss of etcd quorum, or cluster host isolation.
