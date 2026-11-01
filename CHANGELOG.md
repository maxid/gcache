# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

- `Scanner`, an optional key-enumeration port (`Scan(ctx, prefix, fn)`), and its
  Redis implementation: `SCAN` + `MATCH <prefix>*` + `COUNT`, callback error stops
  the scan, store failures wrap `ErrUnavailable`. Olric deliberately does not
  implement it (a DMap walk is a full-partition scan, not a prefix lookup).
- Status documentation corrected: the library is implemented and tested, not a
  scaffold. The remaining gaps are listed in README.md.

## [0.0.0-dev]

- Initial implementation: two-level cache, memory/Redis/Olric shared tiers,
  invalidation broadcast, distributed lock, negative caching, stale-if-error,
  background sweep and health check.
- `SetPersistent` stores a value without the default TTL; memory and Redis
  remote stores honor zero as no expiry.
