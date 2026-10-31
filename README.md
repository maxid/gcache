English | [简体中文](README.zh-CN.md)

# gcache

Two-level cache (local + Redis) and distributed lock toolkit in Go.

> Status: **experimental scaffold** (v0.0.0-dev).

## Shape

```
L1   local LRU + TTL, bounded by entries and bytes   (in-process, always on)
L2   RemoteStore port                                (redis/ or olric/, pluggable)
     └─ same Redis the Java runtime uses, or an embedded Olric cluster
INV  Invalidator port                                (redis pub/sub, olric pub/sub)
LOCK Locker port                                     (redislock: SET NX + Lua)
```

This mirrors the platform's Java cache (Caffeine L1 + Redis L2 + broadcast
invalidation), so a Go process and a Java process can share one Redis. Both
layers are active at once: L1 answers locally, L2 is the source of truth for a
miss. Using an embedded cluster instead of Redis only swaps the L2 port.

## Status

| Piece | Status |
|---|---|
| `Cache` with `Get`/`Set`/`Delete`/`GetOrLoad`, bounded L1, copied values | done |
| TTL clamping, jitter, negative caching, size rejection | done |
| Per-key load collapsing with per-waiter cancellation | done |
| Invalidation port + `InvalidationError` (applied but not propagated) | done |
| `Lock`/`Locker`/`WaitPolicy` ports | done |
| `memory/` store and bus (tests, single process) | done |
| Redis `RemoteStore`/`Invalidator` | done (miniredis tests) |
| Redis `Locker`: script path + native `DELEX`/`SET IFEQ` path | done |
| Olric `RemoteStore` (embedded cluster as the L2) | done, verified against a real node (`-tags=integration`) |
| Negative caching: local absence markers, cleared by every write | done |
| Loader timeout per load (`LoaderTimeout`) | done |
| Background expiry sweep (`SweepInterval`) | done |
| `stale-if-error` with a bounded window, served entries marked `Stale` | done |
| Health check (`Health`, store `Ping`) | done |


### Locking without Lua

`SET key token NX PX ttl` is atomic in every Redis version, so acquire never needed
scripting. Release and renew must *compare the token first*:

| Redis | release | renew |
|---|---|---|
| < 8.4 | `EVAL if get==token then del` | `EVAL if get==token then pexpire` |
| **8.4+** | `DELEX key IFEQ <token>` | `SET key token IFEQ <token> PX <ttl>` |

`Locker` probes `COMMAND INFO delex` once and picks a path, so one binary works on
both. Note that Redis 7's *Functions* (`FUNCTION LOAD`) are still Lua — they only
change how the script is managed, not whether Lua runs. Valkey's `DELIFEQ` and
Tair's `CAS`/`CAD` are not `DELEX`, so they stay on the script path.

Rules the implementation keeps:

- a missing entry is `(entry, false, nil)`, never an error; a broken store is an
  error and is never disguised as "absent";
- loader failures are not cached; only an explicit `ErrNotFound` may be cached,
  briefly;
- `Set`/`Delete` write L2 first, then L1, then broadcast; a failed broadcast is
  `InvalidationError{Applied: true}` — the write happened, other nodes may be
  stale until TTL;
- values are copied on the way in and out, so a caller cannot mutate a cached
  buffer;
- invalidation carries key and version only, never the value.

## Install

```bash
go get github.com/maxid/gcache
```

## License

MIT. See [LICENSE](LICENSE).
