// Package gcache is a two-level cache: a bounded in-process tier (L1) in front of
// a pluggable shared store (L2), with a pluggable invalidation channel, an
// optional key-enumeration port and a distributed lock port.
//
// Status: implemented (v0.0.0-dev, pre-1.0, so the API may still change). See
// README.md for what is covered by the test suite and what remains partial.
package gcache
