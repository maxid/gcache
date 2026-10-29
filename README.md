# gcache

Two-level cache (local + Redis) and distributed lock toolkit in Go, equivalent in intent to the distributed cache used by XDMP.

> Status: **experimental scaffold** (v0.0.0-dev).

## Planned features

- Local cache layer (LRU/TTL)
- Redis second-level cache
- Cross-node invalidation (pub/sub)
- Distributed locks (reentrant helpers)
- Cache statistics and health checks

## Install

```bash
go get github.com/maxid/gcache
```

## License

MIT. See [LICENSE](LICENSE).
