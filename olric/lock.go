package olric

// The distributed lock over an Olric DMap.
//
// gcache's Locker contract has one non-negotiable rule: releasing a lock must
// compare the token first, because a lock that expired and was taken by someone
// else must not be deleted by its previous owner. Olric's DMap gives exactly that
// primitive, and gives it the same way Redis does — `PUT key token NX PX ttl` for
// acquire, and release/renew that compare the stored value to the token before
// touching it (the server answers ErrNoSuchLock when the token no longer matches,
// which maps to gcache.ErrLockLost).
//
// Two consequences are worth stating plainly:
//
//   - there is no quorum. This is a single-key lock on the partition owner, the
//     same construction as a single-Redis lock, not Redlock across independent
//     masters. A partition owner change or a lost node during the critical section
//     is the failure mode, and fencing is what protects the resource against it;
//   - Lock.Token is local. Olric keeps the owner token server-side and does not
//     expose it, so Token() identifies this holder within the process (logs, tests)
//     while ownership itself is enforced by the stored value.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	olricdb "github.com/olric-data/olric"

	"github.com/maxid/gcache"
)

// LockingDMap is a DMap that can also take Olric's distributed lock and mint a
// monotonic fence. Store needs only DMap; Locker needs this one.
type LockingDMap interface {
	DMap
	// Lock takes the lock for key for at most ttl, waiting up to wait for it. A wait
	// of zero tries once. A lock that is not renewed expires with its ttl, after
	// which its handle's Unlock and Renew report gcache.ErrLockLost.
	Lock(ctx context.Context, key string, ttl, wait time.Duration) (LockHandle, error)
	// Incr atomically adds delta to the integer stored at key and returns the new
	// value. The locker uses it to mint fencing tokens.
	Incr(ctx context.Context, key string, delta int) (int, error)
}

// LockHandle is one held lock, bound to the token the cluster stored.
type LockHandle interface {
	// Unlock releases the lock, or reports gcache.ErrLockLost when the stored token
	// is no longer this holder's.
	Unlock(ctx context.Context) error
	// Renew extends the lease, with the same ownership rule as Unlock.
	Renew(ctx context.Context, ttl time.Duration) error
}

// Locker implements gcache.Locker over a LockingDMap.
type Locker struct {
	dmap       LockingDMap
	fencingKey string
}

// Locker implements gcache.Locker. The assertion lives next to the implementation,
// so a refactor that drops a method fails to compile.
var _ gcache.Locker = (*Locker)(nil)

// NewLocker creates a locker. Fencing tokens are allocated from fencingKey
// (incremented atomically) so a protected resource can reject a stale holder;
// "gcache:fencing" is used when it is empty.
func NewLocker(dmap LockingDMap, fencingKey string) (*Locker, error) {
	if dmap == nil {
		return nil, fmt.Errorf("%w: LockingDMap is required", gcache.ErrInvalidConfig)
	}
	if fencingKey == "" {
		fencingKey = "gcache:fencing"
	}
	return &Locker{dmap: dmap, fencingKey: fencingKey}, nil
}

// TryAcquire implements gcache.Locker: one attempt, no wait.
func (l *Locker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (gcache.Lock, bool, error) {
	if ttl <= 0 {
		return nil, false, fmt.Errorf("%w: lock TTL must be positive", gcache.ErrInvalidConfig)
	}
	handle, err := l.dmap.Lock(ctx, key, ttl, 0)
	if err != nil {
		if errors.Is(err, gcache.ErrLockNotAcquired) {
			return nil, false, nil
		}
		return nil, false, unavailable(err)
	}
	return l.mint(ctx, key, handle, ttl)
}

// Acquire implements gcache.Locker. Olric retries the acquire itself every 10 ms
// until the wait deadline, so WaitPolicy's backoff fields do not apply here; only
// Timeout bounds the wait.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration, wait gcache.WaitPolicy) (gcache.Lock, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: lock TTL must be positive", gcache.ErrInvalidConfig)
	}
	if wait.Timeout <= 0 {
		wait = gcache.DefaultWaitPolicy
	}
	handle, err := l.dmap.Lock(ctx, key, ttl, wait.Timeout)
	if err != nil {
		if errors.Is(err, gcache.ErrLockNotAcquired) {
			return nil, fmt.Errorf("%w: %s", gcache.ErrLockNotAcquired, key)
		}
		return nil, unavailable(err)
	}
	held, ok, err := l.mint(ctx, key, handle, ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", gcache.ErrLockNotAcquired, key)
	}
	return held, nil
}

// mint turns a held handle into a gcache.Lock. A failed fence releases the lock
// instead of handing back one whose ownership cannot be proven to a resource.
func (l *Locker) mint(ctx context.Context, key string, handle LockHandle, ttl time.Duration) (gcache.Lock, bool, error) {
	fencing, err := l.dmap.Incr(ctx, l.fencingKey, 1)
	if err != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = handle.Unlock(releaseCtx)
		return nil, false, unavailable(err)
	}
	token, err := newToken()
	if err != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = handle.Unlock(releaseCtx)
		return nil, false, err
	}
	return &lock{
		handle:    handle,
		key:       key,
		token:     token,
		fencing:   uint64(fencing),
		expiresAt: time.Now().Add(ttl),
	}, true, nil
}

// unavailable wraps a backend failure the way every other method of this package
// does: "the store is broken" must never look like "the lock is held".
func unavailable(err error) error { return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err) }

// lock is one held lock.
type lock struct {
	handle LockHandle

	key     string
	token   string
	fencing uint64

	mu        sync.Mutex
	expiresAt time.Time
	released  bool
}

// Token implements gcache.Lock. It identifies the holder within this process;
// ownership itself is the token Olric stored.
func (l *lock) Token() string { return l.token }

// FencingToken implements gcache.Lock.
func (l *lock) FencingToken() uint64 { return l.fencing }

// ExpiresAt implements gcache.Lock.
func (l *lock) ExpiresAt() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expiresAt
}

// Renew implements gcache.Lock.
func (l *lock) Renew(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("%w: lock TTL must be positive", gcache.ErrInvalidConfig)
	}
	if err := l.handle.Renew(ctx, ttl); err != nil {
		return err
	}
	l.mu.Lock()
	l.expiresAt = time.Now().Add(ttl)
	l.mu.Unlock()
	return nil
}

// Unlock implements gcache.Lock. A second call is not an error: the caller already
// got what it asked for, and reporting ErrLockLost would turn its own release into
// a lost-lock alarm.
func (l *lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()

	if err := l.handle.Unlock(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	l.released = true
	l.mu.Unlock()
	return nil
}

// newToken returns a random holder id, 32 hex characters.
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("gcache/olric: token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// FromLockingDMap adapts a real olric.DMap for both the store and the locker.
func FromLockingDMap(dmap olricdb.DMap) LockingDMap { return olricBridge{dmap: dmap} }

// Both capabilities come from the same bridge: Olric's DMap already has everything
// a lock needs, so there is no second adapter to keep in sync.
var _ LockingDMap = olricBridge{}

func (b olricBridge) Lock(ctx context.Context, key string, ttl, wait time.Duration) (LockHandle, error) {
	handle, err := b.dmap.LockWithTimeout(ctx, key, ttl, wait)
	if err != nil {
		if errors.Is(err, olricdb.ErrLockNotAcquired) {
			return nil, gcache.ErrLockNotAcquired
		}
		return nil, err
	}
	return olricLockHandle{handle: handle}, nil
}

func (b olricBridge) Incr(ctx context.Context, key string, delta int) (int, error) {
	return b.dmap.Incr(ctx, key, delta)
}

// olricLockHandle wraps the cluster's handle: ErrNoSuchLock is the cluster saying
// "the stored token is not yours any more", which is gcache's ErrLockLost.
type olricLockHandle struct{ handle olricdb.LockContext }

func (h olricLockHandle) Unlock(ctx context.Context) error {
	if err := h.handle.Unlock(ctx); err != nil {
		return lockError(err)
	}
	return nil
}

func (h olricLockHandle) Renew(ctx context.Context, ttl time.Duration) error {
	if err := h.handle.Lease(ctx, ttl); err != nil {
		return lockError(err)
	}
	return nil
}

func lockError(err error) error {
	if errors.Is(err, olricdb.ErrNoSuchLock) {
		return fmt.Errorf("%w: %v", gcache.ErrLockLost, err)
	}
	return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
}
