// Package gcache is a two-level cache: a bounded in-process tier (L1) in front of
// a pluggable shared store (L2), with a pluggable invalidation channel and a
// distributed lock port.
//
// The shape mirrors the platform's Java cache (Caffeine L1 + Redis L2 + broadcast
// invalidation), because a Go process and a Java process commonly run side by side
// against the same Redis. It is a neutral library: key naming, tenant scoping and
// what may be cached stay with the consumer.
//
// It does not promise cross-node linearizability, cache/DB transactions, or
// absolute mutual exclusion after a lock expires.
package gcache

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors are distinct so callers can tell "not there" from "broken".
var (
	// ErrNotFound is returned by loaders when the source has no such record.
	ErrNotFound = errors.New("gcache: not found")
	// ErrUnavailable means the shared store (or its network path) is unusable.
	ErrUnavailable = errors.New("gcache: remote store unavailable")
	// ErrTooLarge means a value exceeds the configured limit; it is rejected
	// before it reaches memory or the network.
	ErrTooLarge = errors.New("gcache: value too large")
	// ErrClosed means the cache was closed.
	ErrClosed = errors.New("gcache: cache is closed")
	// ErrInvalidConfig means the configuration can never work.
	ErrInvalidConfig = errors.New("gcache: invalid configuration")
	// ErrLockNotAcquired means the lock is held elsewhere.
	ErrLockNotAcquired = errors.New("gcache: lock not acquired")
	// ErrLockLost means the lock is no longer ours (expired or taken over).
	ErrLockLost = errors.New("gcache: lock lost")
)

// InvalidationError reports a write that succeeded remotely but whose
// invalidation broadcast failed. Applied is always true for this error: the entry
// is written, other nodes may serve a stale copy until its TTL expires.
type InvalidationError struct {
	Key string
	Err error
}

func (e *InvalidationError) Error() string {
	return fmt.Sprintf("gcache: %q applied but invalidation failed: %v", e.Key, e.Err)
}

// Unwrap exposes the underlying failure.
func (e *InvalidationError) Unwrap() error { return e.Err }

// Applied always reports true: this error only exists after the write succeeded.
func (e *InvalidationError) Applied() bool { return true }

// Config bounds the cache. Every field has a safe default; the zero Config is
// valid after Normalize.
type Config struct {
	// Namespace prefixes every key. Two caches sharing a store must not collide.
	Namespace string
	// DefaultTTL applies when a Set passes zero. MaxTTL caps any TTL.
	DefaultTTL time.Duration
	MaxTTL     time.Duration
	// Jitter randomises TTLs by ±Jitter to keep entries from expiring together.
	// Zero disables it.
	Jitter float64

	// LocalMaxEntries and LocalMaxBytes bound the in-process tier. Reaching
	// either evicts by LRU.
	LocalMaxEntries int
	LocalMaxBytes   int64

	// NegativeTTL caches "not found" results. Zero disables negative caching.
	NegativeTTL time.Duration
	// RemoteMaxValueBytes rejects oversized values before they reach the store.
	RemoteMaxValueBytes int64
	// LoaderTimeout bounds one loader run.
	LoaderTimeout time.Duration
	// InvalidationBuffer bounds the subscriber queue; a slow subscriber drains
	// the local tier rather than growing without limit.
	InvalidationBuffer int
}

// Defaults are the values Normalize applies.
const (
	DefaultTTL         = 5 * time.Minute
	DefaultMaxTTL      = 24 * time.Hour
	DefaultMaxEntries  = 10_000
	DefaultMaxBytes    = 64 << 20
	DefaultMaxValue    = 1 << 20
	DefaultLoadTimeout = 5 * time.Second
	DefaultInvBuffer   = 1024
)

// Normalize fills defaults and rejects impossible combinations.
func (c *Config) Normalize() error {
	c.Namespace = strings.TrimSpace(c.Namespace)
	if c.Namespace == "" {
		return fmt.Errorf("%w: namespace is required", ErrInvalidConfig)
	}
	if strings.ContainsAny(c.Namespace, " {}[]") {
		return fmt.Errorf("%w: namespace %q contains an unsupported character", ErrInvalidConfig, c.Namespace)
	}

	if c.DefaultTTL <= 0 {
		c.DefaultTTL = DefaultTTL
	}
	if c.MaxTTL <= 0 {
		c.MaxTTL = DefaultMaxTTL
	}
	if c.DefaultTTL > c.MaxTTL {
		return fmt.Errorf("%w: default TTL %s exceeds max TTL %s", ErrInvalidConfig, c.DefaultTTL, c.MaxTTL)
	}
	if c.Jitter < 0 || c.Jitter >= 1 {
		return fmt.Errorf("%w: jitter %v outside [0,1)", ErrInvalidConfig, c.Jitter)
	}
	if c.LocalMaxEntries <= 0 {
		c.LocalMaxEntries = DefaultMaxEntries
	}
	if c.LocalMaxBytes <= 0 {
		c.LocalMaxBytes = DefaultMaxBytes
	}
	if c.RemoteMaxValueBytes <= 0 {
		c.RemoteMaxValueBytes = DefaultMaxValue
	}
	if c.LoaderTimeout <= 0 {
		c.LoaderTimeout = DefaultLoadTimeout
	}
	if c.InvalidationBuffer <= 0 {
		c.InvalidationBuffer = DefaultInvBuffer
	}
	if c.NegativeTTL < 0 {
		return fmt.Errorf("%w: negative TTL must not be negative", ErrInvalidConfig)
	}
	if c.NegativeTTL > c.MaxTTL {
		return fmt.Errorf("%w: negative TTL %s exceeds max TTL %s", ErrInvalidConfig, c.NegativeTTL, c.MaxTTL)
	}
	return nil
}
