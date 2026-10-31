package gcache

import (
	"context"
	"time"
)

// Lock is one held lock. Token identifies the owner; FencingToken is a
// monotonically increasing value a protected resource can compare against, so a
// lock that silently expired cannot be mistaken for exclusivity.
type Lock interface {
	Token() string
	FencingToken() uint64
	ExpiresAt() time.Time
	// Renew extends the lease. It fails with ErrLockLost when the token is no
	// longer the owner.
	Renew(ctx context.Context, ttl time.Duration) error
	// Unlock releases the lock. It fails with ErrLockLost when the token no
	// longer matches; a plain DELETE is forbidden because it would release
	// someone else's lock.
	Unlock(ctx context.Context) error
}

// Locker acquires distributed locks.
type Locker interface {
	TryAcquire(ctx context.Context, key string, ttl time.Duration) (Lock, bool, error)
	Acquire(ctx context.Context, key string, ttl time.Duration, wait WaitPolicy) (Lock, error)
}

// WaitPolicy bounds how long Acquire retries before giving up.
type WaitPolicy struct {
	Timeout    time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// DefaultWaitPolicy is a short bounded wait: a task that cannot get the lock
// quickly should reschedule rather than queue behind an unbounded retry.
var DefaultWaitPolicy = WaitPolicy{
	Timeout:    5 * time.Second,
	MinBackoff: 10 * time.Millisecond,
	MaxBackoff: 250 * time.Millisecond,
}

// Normalize fills defaults.
func (w *WaitPolicy) Normalize() {
	if w.Timeout <= 0 {
		w.Timeout = DefaultWaitPolicy.Timeout
	}
	if w.MinBackoff <= 0 {
		w.MinBackoff = DefaultWaitPolicy.MinBackoff
	}
	if w.MaxBackoff <= 0 {
		w.MaxBackoff = DefaultWaitPolicy.MaxBackoff
	}
}
