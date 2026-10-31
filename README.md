English | [简体中文](README.zh-CN.md)

# gcache

Two-level cache (local + Redis) and distributed lock toolkit in Go.

> Status: **experimental scaffold** (v0.0.0-dev).

## Planned features

- Local cache layer (LRU/TTL)
- Redis second-level cache
- Cross-node invalidation (pub/sub)
- Distributed locks
- Cache statistics and health checks

## Install

```bash
go get github.com/maxid/gcache
```

## License

MIT. See [LICENSE](LICENSE).
