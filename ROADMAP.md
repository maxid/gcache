# Roadmap

- [x] Local cache layer (bounded LRU + TTL, stale-if-error window)
- [x] Redis layer (`RemoteStore`, `Invalidator`, `Locker`)
- [x] Invalidation broadcast (Redis pub/sub and the in-process bus)
- [x] Distributed lock (`Locker`/`Lock`, compare-the-token release and renew)
- [x] Prefix scan port (`Scanner`) with a Redis implementation
- [ ] Benchmarks
- [ ] v1.0 API stabilization
