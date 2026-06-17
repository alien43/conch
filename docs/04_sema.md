# 04 — `conch sema`

Counting semaphore: at most N concurrent holders cluster-wide. `--max 1` is a lock.

## Synopsis

```
conch sema <name> --max N [--ttl 10s] [--wait 30s|--nonblock] [--spread] -- <cmd...>
conch sema <name> --max N --who [--json]
```

## The recipe

The one primitive not provided by the etcd `concurrency` package (~100 lines):

1. Open session; `Txn` put of holder JSON at
   `/conch/v1/sema/<name>/<max>/<leaseID>` bound to the session lease
   (create-if-absent; the leaseID key suffix makes it naturally unique).
2. Fetch all keys under the prefix sorted by create-revision. Let `r` = rank of our
   key (0-based).
3. `r < N` ⇒ **we hold a slot**, proceed to the supervision loop.
4. `r ≥ N` ⇒ wait: watch for deletion of the key at rank `r − N` (the one whose
   departure can promote us), then re-evaluate from step 2. This wakes exactly one
   waiter per release — no thundering herd.
5. Release = delete our key (clean path) or lease expiry (crash path).

Fencing token: the create-revision of our key (exported as `CONCH_REV`).

## `--max` is part of the name

The capacity is encoded in the key prefix. Two nodes invoking the same `<name>` with
different `--max` land in *different* semaphores — a loud, debuggable mistake
(`--who` shows both) instead of silent oversubscription. The semaphore's identity is
`(name, max)`; changing capacity means draining the old one.

## `--spread` (reserved for the scheduler)

With `--spread`, the key becomes `/conch/v1/sema/<name>/<max>/<host>` (still
lease-bound): each node can hold at most one slot, so N slots ⇒ N *distinct nodes*.
Acquisition by a node already holding fails as if no slot were free. This is the
"replicas with anti-affinity" building block from `00_design.md` §7. Specified now so
the key schema accounts for it; implemented when the scheduler needs it.

## `--who`

One line per key under the prefix, in revision order, annotated:

```
juicefs-rebalance[2/2] HELD  dell-server pid=4242 since=2026-06-12T10:15:00Z rev=8812
juicefs-rebalance[2/2] HELD  rpi-server  pid=991  since=2026-06-12T10:15:04Z rev=8814
juicefs-rebalance[2/2] WAIT  asus-server pid=1207 since=2026-06-12T10:15:09Z rev=8815
```

Exit 0 if any holders, 1 if the semaphore is empty.

## Flags

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--max` | **required** | capacity N ≥ 1 |
| `--wait` | infinite | max time to wait for a slot (exit 75 on timeout; our waiter key is deleted) |
| `--nonblock` | off | `--wait 0`, flock -n style |
| `--spread` | off | at most one slot per node (see above) |
| `--kill-after` | `5s` | SIGTERM → SIGKILL escalation |

## Failure behavior

* Same overlap window as `elect`: a crashed holder's slot frees only at lease expiry,
  and a promoted waiter may start while the crashed holder's child is being killed.
  Capacity can therefore briefly read N+1 from the outside. Same contract: idempotent
  or crash-tolerant commands.
* A waiter that loses its session while queued re-enters from step 1 (new key, back of
  the queue) if `--wait` allows, else exits 75. Queue position is not preserved across
  session loss — simplicity over fairness.

## Examples

```sh
# classic lock around a mutating script
conch sema certs-renew --max 1 --nonblock -- ./renew.sh || echo "already running elsewhere"

# at most 2 nodes rebalance concurrently
conch sema juicefs-rebalance --max 2 -- juicefs gc --delete

# inspect
conch sema juicefs-rebalance --max 2 --who
```
