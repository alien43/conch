# 07 — Deployment & Rollout

No external servers, no dynamic config, no state storage. Deployment is pure NixOS.

## 1. Nix flake output

Conch is built directly from the top-level repository flake:

```sh
nix build .#conch                     # builds tools/conch → bin/conch
nix flake check                       # runs static checks + unit tests + integration
```

## 2. Dynamic binary distribution

For environments where Nix isn't fully set up or a developer needs a quick binary:

```sh
# Statically-linked Go binary, architecture-independent
CGO_ENABLED=0 go build -ldflags="-s -w" -o conch ./cmd/conch
```

This binary has zero dynamic loader dependencies, making it safe to copy to any target host of the same CPU architecture (e.g. from debian to NixOS).

## 3. NixOS module — `nixos/modules/services/conch.nix`

Same shape as the neighbours (`etcd.nix`, `patroni.nix`):

```nix
services.conch = {
  enable = true;                       # installs CLI + sets CONCH_ENDPOINTS globally
  conchd.enable = true;                # the cron daemon
  jobs = {                             # declarative jobs (optional, see below)
    restic-juicefs = { schedule = "*/15 * * * *"; cmd = [ "restic" "backup" "/mnt/juicefs" ]; };
  };
};
```

* **CLI**: `environment.systemPackages = [ pkgs.conch ]` plus
  `environment.variables.CONCH_ENDPOINTS` derived from the same node list
  `cluster-node.nix` already feeds etcd — one source of truth for endpoints.
* **conchd unit**: `After=network-online.target etcd.service` (wants, not requires —
  conchd must survive local etcd dying while quorum lives elsewhere), `Restart=always`,
  `RestartSec=5`. Runs as **root** initially: jobs and future services legitimately
  need incus/system access; tighten later per-job rather than pretending otherwise
  with a DynamicUser that can't do its work.
* **Declarative jobs**: `jobs` renders to a oneshot activation script that does
  `conch cron add` for each entry (idempotent — add overwrites). Imperative
  `conch cron add` from anywhere remains first-class; declarative is just nix-managed
  convenience on top. Jobs removed from nix are *not* auto-removed from etcd (no
  state tracking in v1; `cron ls` makes drift visible).

## 4. Rollout order

1. Package + CLI on all three nodes (`upgrade_all.sh` as usual). Nothing runs yet —
   pure binary delivery. Verify: `conch elect rollout-test --who` from each node.
2. Run `conch-smoke.sh` from one node against real quorum.
3. Enable `conchd` on all nodes; migrate one harmless cron job in and watch a few
   ticks (`conch cron ls --last`).
4. Migrate remaining jobs; later milestones (scheduler, 00_design §7) ride the same
   units — no new deployment machinery.

Upgrades are plain `nixos-rebuild` per node. conchd restarts skip no ticks unless all
three nodes restart within one tick window (05_cron §failure). Key schema versioning
(`/conch/v1/`) means an incompatible future version deploys side-by-side and jobs
migrate by re-`add` — never an in-place schema migration.

## 5. Observability

* **Logs**: journald per node (`journalctl -u conch-conchd`); child output included.
* **Liveness**: monit (already cluster-standard) gets a check per node:
  `conch elect monit-probe --who` exercises CLI→etcd; process check on conchd.
* **Alerting**: a conch cron job is the heartbeat —
  `conch cron add heartbeat --schedule '@every 5m' -- curl -fsS $NTFY_TOPIC...` to the
  existing ntfy (Services VIP). If the heartbeat goes quiet, either cron, etcd quorum,
  or the cluster node is dead.
