# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- `Scanner`, an optional key-enumeration port (`Scan(ctx, prefix, fn)`), with both
  shipped implementations: `SCAN` + `MATCH <prefix>*` + `COUNT` on Redis, and a
  paged DMap walk (`Match` + `Count`, 512 keys per round trip) on Olric. The
  callback's error stops the scan and is returned unchanged, a store failure wraps
  `ErrUnavailable` and never looks like an empty key list. **Breaking:** `olric.DMap`
  gained `Scan`, so an implementation of that exported port outside this repository
  no longer satisfies it; `olric.Store` now implements `gcache.Scanner` as well.
- Status documentation corrected: the library is implemented and tested, not a
  scaffold. The remaining gaps are listed in README.md.

## [0.0.0-dev]

- Initial implementation: two-level cache, memory/Redis/Olric shared tiers,
  invalidation broadcast, distributed lock, negative caching, stale-if-error,
  background sweep and health check.
- `SetPersistent` stores a value without the default TTL; memory and Redis
  remote stores honor zero as no expiry.
