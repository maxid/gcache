package olric_test

// The lock over the Olric port. The fake models what the cluster guarantees — one
// holder per key, release only with the stored token — so a test can drive the two
// cases the contract cares about without a node: a second holder is refused, and a
// handle whose lock is gone reports ErrLockLost instead of deleting someone else's.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gcache"
	golric "github.com/maxid/gcache/olric"
)

type fakeLockingDMap struct {
	*fakeDMap

	mu       sync.Mutex
	locks    map[string]string
	fence    int
	lockErr  error
	incrErr  error
	onUnlock func()
}

func newFakeLocking() *fakeLockingDMap {
	return &fakeLockingDMap{fakeDMap: newFake(), locks: map[string]string{}}
}

// Lock retries for up to wait, the way the cluster does; a zero wait tries once.
func (f *fakeLockingDMap) Lock(_ context.Context, key string, _ time.Duration, wait time.Duration) (golric.LockHandle, error) {
	if f.lockErr != nil {
		return nil, f.lockErr
	}
	deadline := time.Now().Add(wait)
	for {
		f.mu.Lock()
		if _, held := f.locks[key]; !held {
			token := key + "-token"
			f.locks[key] = token
			f.mu.Unlock()
			return fakeLockHandle{dm: f, key: key, token: token}, nil
		}
		f.mu.Unlock()
		if !time.Now().Before(deadline) {
			return nil, gcache.ErrLockNotAcquired
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fakeLockingDMap) Incr(_ context.Context, _ string, delta int) (int, error) {
	if f.incrErr != nil {
		return 0, f.incrErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fence += delta
	return f.fence, nil
}

// expire drops the lock the way the cluster does when its TTL elapses.
func (f *fakeLockingDMap) expire(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.locks, key)
}

func (f *fakeLockingDMap) held(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.locks[key]
	return ok
}

type fakeLockHandle struct {
	dm    *fakeLockingDMap
	key   string
	token string
}

func (h fakeLockHandle) Unlock(_ context.Context) error {
	h.dm.mu.Lock()
	defer h.dm.mu.Unlock()
	if h.dm.locks[h.key] != h.token {
		return gcache.ErrLockLost
	}
	delete(h.dm.locks, h.key)
	if h.dm.onUnlock != nil {
		h.dm.onUnlock()
	}
	return nil
}

func (h fakeLockHandle) Renew(_ context.Context, _ time.Duration) error {
	h.dm.mu.Lock()
	defer h.dm.mu.Unlock()
	if h.dm.locks[h.key] != h.token {
		return gcache.ErrLockLost
	}
	return nil
}

func TestLockerIsExclusive(t *testing.T) {
	dmap := newFakeLocking()
	locker, err := golric.NewLocker(dmap, "")
	if err != nil {
		t.Fatalf("locker: %v", err)
	}
	ctx := context.Background()

	held, ok, err := locker.TryAcquire(ctx, "job:42", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first acquire = %v, %v", ok, err)
	}
	if _, ok, err := locker.TryAcquire(ctx, "job:42", time.Minute); ok || err != nil {
		t.Fatalf("second acquire = %v, %v (want false, nil)", ok, err)
	}

	if err := held.Unlock(ctx); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := held.Unlock(ctx); err != nil {
		t.Fatalf("second unlock: %v (a repeat release is not a lost lock)", err)
	}
	if _, ok, err := locker.TryAcquire(ctx, "job:42", time.Minute); !ok || err != nil {
		t.Fatalf("acquire after release = %v, %v", ok, err)
	}
}

// The rule the whole port exists for: a lock that expired and was taken by someone
// else must not be released (or renewed) by its previous holder.
func TestLockerReportsLockLost(t *testing.T) {
	dmap := newFakeLocking()
	locker, _ := golric.NewLocker(dmap, "")
	ctx := context.Background()

	held, ok, err := locker.TryAcquire(ctx, "job:7", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	dmap.expire("job:7") // the TTL elapsed, or another node took it

	if err := held.Renew(ctx, time.Minute); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("renew after expiry = %v, want ErrLockLost", err)
	}
	if err := held.Unlock(ctx); !errors.Is(err, gcache.ErrLockLost) {
		t.Fatalf("unlock after expiry = %v, want ErrLockLost", err)
	}
}

func TestLockerMintsFencingTokens(t *testing.T) {
	dmap := newFakeLocking()
	locker, _ := golric.NewLocker(dmap, "fence")
	ctx := context.Background()

	first, ok, err := locker.TryAcquire(ctx, "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire a = %v, %v", ok, err)
	}
	second, ok, err := locker.TryAcquire(ctx, "b", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire b = %v, %v", ok, err)
	}
	if first.FencingToken() >= second.FencingToken() {
		t.Fatalf("fencing %d then %d, want strictly increasing", first.FencingToken(), second.FencingToken())
	}
	if first.Token() == "" || first.Token() == second.Token() {
		t.Fatalf("tokens = %q, %q, want two distinct holders", first.Token(), second.Token())
	}
	if first.ExpiresAt().Before(time.Now()) {
		t.Fatalf("expires at %s, want the future", first.ExpiresAt())
	}
}

// A lock whose fence could not be minted is released, not handed out: a resource
// cannot reject a stale holder without the number.
func TestLockerReleasesWhenFencingFails(t *testing.T) {
	dmap := newFakeLocking()
	dmap.incrErr = errors.New("cluster unreachable")
	locker, _ := golric.NewLocker(dmap, "")
	ctx := context.Background()

	held, ok, err := locker.TryAcquire(ctx, "job", time.Minute)
	if ok || !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("acquire = %v, %v, want ErrUnavailable", ok, err)
	}
	if held != nil {
		t.Fatalf("lock = %v, want nil", held)
	}
	if dmap.held("job") {
		t.Fatal("the lock stayed held after the fence failed")
	}
}

func TestLockerMapsBackendFailuresToUnavailable(t *testing.T) {
	dmap := newFakeLocking()
	dmap.lockErr = errors.New("cluster unreachable")
	locker, _ := golric.NewLocker(dmap, "")

	_, ok, err := locker.TryAcquire(context.Background(), "job", time.Minute)
	if ok || !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("acquire = %v, %v, want ErrUnavailable", ok, err)
	}
	if _, err := locker.Acquire(context.Background(), "job", time.Minute, gcache.WaitPolicy{Timeout: time.Millisecond}); !errors.Is(err, gcache.ErrUnavailable) {
		t.Fatalf("acquire err = %v, want ErrUnavailable", err)
	}
}

func TestNewLockerValidates(t *testing.T) {
	if _, err := golric.NewLocker(nil, ""); !errors.Is(err, gcache.ErrInvalidConfig) {
		t.Fatalf("nil dmap err = %v", err)
	}
	locker, _ := golric.NewLocker(newFakeLocking(), "")
	if _, _, err := locker.TryAcquire(context.Background(), "job", 0); !errors.Is(err, gcache.ErrInvalidConfig) {
		t.Fatalf("zero ttl err = %v", err)
	}
	if _, err := locker.Acquire(context.Background(), "job", -time.Second, gcache.WaitPolicy{}); !errors.Is(err, gcache.ErrInvalidConfig) {
		t.Fatalf("negative ttl err = %v", err)
	}
}

func TestLockerAcquireTimesOut(t *testing.T) {
	dmap := newFakeLocking()
	locker, _ := golric.NewLocker(dmap, "")
	ctx := context.Background()

	if _, ok, err := locker.TryAcquire(ctx, "job", time.Minute); err != nil || !ok {
		t.Fatalf("acquire = %v, %v", ok, err)
	}
	start := time.Now()
	_, err := locker.Acquire(ctx, "job", time.Minute, gcache.WaitPolicy{Timeout: 50 * time.Millisecond})
	if !errors.Is(err, gcache.ErrLockNotAcquired) {
		t.Fatalf("acquire held lock = %v, want ErrLockNotAcquired", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("acquire returned after %s, want it to wait for the policy", elapsed)
	}
}
