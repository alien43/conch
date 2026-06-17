# 01 — Coding principles

The point of these rules is that the same supervision loop that runs a cron job will
eventually run every service in the cluster. Boring and correct beats clever and rich.

## 1. One binary, one primitive per subcommand

* Single Go module, single binary `conch`; subcommands are thin CLI shells over
  `internal/` packages. Busybox-style argv[0] symlinks may come later; not now.
* Each subcommand wraps exactly one primitive. A feature that needs two primitives is a
  *composition* and belongs in the shell (or a future subcommand that visibly composes),
  never as a flag that mutates a primitive's semantics.

## 2. Dependency allowlist

```
go.etcd.io/etcd/client/v3        # incl. concurrency package
github.com/robfig/cron/v3        # schedule parsing only
```

Everything else is stdlib: `flag` for CLI (subcommand dispatch by hand, no cobra),
`log/slog` for logging, `os/exec` for children, `encoding/json` for values. A new
dependency requires editing this file in the same PR — that friction is the feature.

## 3. Fail closed, always

* Never run the child without a confirmed hold; never *keep* it running without one.
* Keepalive failure is loss **now**, not at TTL expiry. Ambiguity (etcd unreachable,
  session state unknown) is loss.
* On any internal panic/error in the wrapper while a child runs: kill the child, then
  release, then exit non-zero. Order matters.

## 4. The supervision core is sacred

`internal/core` (session + hold + supervise + kill) is written once and reused by every
tool. Rules specific to it:

* No tool-specific logic leaks in. Its API is roughly
  `Run(ctx, hold Holder, cmd []string) (exitCode int, err error)`.
* Every state transition (acquired, lost, child-exit, term-sent, kill-sent) is logged
  with the etcd revision in hand at the time.
* Changes to it require the invariant tests in `06_testing.md` §3 to pass — no
  exceptions, no "just a refactor".

## 5. Contracts over configuration

* No config files. Flags + environment only; every flag has a `CONCH_*` env fallback.
* Exit codes, env vars exported to children, and key schema (see `02_core.md`) are
  **public API**. Changing any of them is a breaking change ⇒ bump the key-schema
  version, support nothing in between.
* Output for humans goes to stderr; output for pipes (`--who`, `ls`) goes to stdout,
  one record per line, stable field order. `--json` where a record has structure.

## 6. Honest about distribution

* The guarantee is *at-most-one-most-of-the-time* (00_design §4). Code comments and docs
  never say "exactly once" — anywhere that overlap matters, name the overlap window.
* Clocks are only trusted to be chrony-close; tick identity is always the *scheduled*
  time, never `time.Now()`.
* All blocking etcd calls take a context with a deadline. There is no retry loop without
  backoff and a cap.

## 7. Small enough to read

* Target: the whole repo readable in one sitting. If `internal/` crosses ~2 kLOC,
  something has gone wrong philosophically, not just technically.
* `go vet`, `gofmt`, `-race` in tests — enforced by `nix flake check`, not by memory.

## 8. Bounded scope (conchd)

conchd is the node's always-on scheduler and read-only status surface. It never supervises offices or semaphores — those are wrapped one-process-each and supervised by the init system. There is deliberately no central supervisor; that flatness is the reliability story.

