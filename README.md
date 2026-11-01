English | [简体中文](README.zh-CN.md)

# gcache

Two-level cache (local + Redis/Olric) and distributed lock toolkit in Go.

> Status: **implemented and tested** (v0.0.0-dev: pre-1.0, so the API may still
> change). Every piece below is exercised by the test suite unless its row says
> otherwise; the gaps that remain are listed under [Open or partial](#open-or-partial).

## Shape

```
L1   local LRU + TTL, bounded by entries and bytes   (in-process, always on)
L2   RemoteStore port                                (redis/, olric/, memory/)
     └─ one Redis shared with the rest of the deployment, or an embedded Olric cluster
INV  Invalidator port                                (redis pub/sub, in-process bus)
LOCK Locker port                                     (redis: SET NX PX + compare-the-token)
SCAN Scanner port (optional)                         (redis: SCAN MATCH <prefix>* COUNT)
```

Both levels are active at once: L1 answers locally, L2 is the source of truth for
a miss. Using an embedded cluster instead of Redis only swaps the L2 port. Key
layout and lock scripts are the conventional ones on purpose: another process
sharing the same Redis should find nothing surprising, not a private scheme.

## Status

| Piece | Status |
|---|---|
| `Cache` with `Get`/`Set`/`Delete`/`GetOrLoad`, bounded L1, copied values | done |
| TTL clamping, jitter, negative caching, size rejection | done |
| `SetPersistent` (zero TTL) | done on memory and Redis; Olric applies its documented default TTL instead |
| Per-key load collapsing with per-waiter cancellation | done |
| Loader timeout per load (`LoaderTimeout`) | done |
| Background expiry sweep (`SweepInterval`) | done |
| `stale-if-error` with a bounded window, served entries marked `Stale` | done |
| Invalidation port + `InvalidationError` (applied but not propagated) | done |
| `Lock`/`Locker`/`WaitPolicy` ports | done |
| `memory/` store and bus (tests, single process) | done |
| Redis `RemoteStore`/`Invalidator` | done (miniredis tests) |
| Redis `Locker`: script path, native `DELEX`/`SET IFEQ` path | done; the script path is tested, the 8.4+ native path is implemented but not exercised by the suite (miniredis has no `DELEX`) |
| Olric `RemoteStore` (embedded cluster as the L2) | done, verified against a real node (`-tags=integration`) |
| Optional key enumeration (`Scanner`) | done on Redis; deliberately absent on Olric (see below) |
| Health check (`Health`, store `Ping`) | Redis store only; `memory` and `olric` do not implement `Ping`, so `Health` is a no-op for them |
| Metrics counters | done except `Metrics.LoadWaiters`, which is declared but never incremented |

### Open or partial

- **Versioned entries.** `Entry.Version` is carried through the local tier and
  the invalidation path, but no shipped store produces a version, so today's
  consistency is TTL + invalidation, not compare-before-backfill.
- **`Config.InvalidationBuffer`.** The field is normalized but not passed to any
  shipped invalidator: the subscriber queue belongs to the implementation
  (`memory.NewInvalidator` takes its own buffer size, Redis pub/sub uses the
  client's channel).
- **Redis 8.4+ lock path.** Implemented and selected by probing `COMMAND INFO
  delex`, but no test runs against a server that has `DELEX`.
- **Olric is a store only.** No `Invalidator`, no `Locker`, no `Scanner`, no
  `Ping` for the Olric-backed tier.
- **Benchmarks and v1.0 API stabilization** are still open; see [ROADMAP.md](ROADMAP.md).

## Scanning the shared tier

`RemoteStore` is key-addressed. Enumeration is a separate, optional port, so a
store can stay key-addressed and the core stays dependency-free:

```go
type Scanner interface {
	Scan(ctx context.Context, prefix string, fn func(key string) error) error
}
```

A store does not have to implement it — the cache never enumerates — so a
consumer asks for it with a type assertion and handles the refusal:

```go
scanner, ok := store.(gcache.Scanner)
if !ok {
	return errors.New("this store cannot enumerate its keys")
}
```

`redis.Store` implements it with `SCAN` + `MATCH <prefix>*` + `COUNT` (512 keys
per round trip). The callback is called once per matching key in an unspecified
order; its error stops the scan and is returned unchanged; a Redis failure is
reported as `ErrUnavailable`, never as an empty key list. `olric.Store`
deliberately does not implement it: an Olric walk visits every partition and
matches each key with a regular expression, so it is not a cheap prefix lookup,
and faking one would hide that cost. See [scanner.go](scanner.go).

## Locking without Lua

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
- invalidation carries key and version only, never the value;
- a scan is paged, and a scan failure is an error, never an empty key list.

## Install

```bash
go get github.com/maxid/gcache
```

## License

MIT. See [LICENSE](LICENSE).
