package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
)

// Locking needs three operations: acquire, release, renew.
//
//   - acquire is `SET key token NX PX ttl` in every Redis version — a single
//     atomic command, no scripting.
//   - release must compare the stored token before deleting, and renew must
//     compare before extending. Until Redis 8.4 (`DELEX ... IFEQ` and
//     `SET ... IFEQ`) that comparison had to happen inside Lua; the scripts below
//     are that fallback.
//
// The Locker probes the server once and uses whichever is available, so the same
// binary works on 6.x/7.x and on 8.4+ without configuration.
const (
	releaseScript = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	renewScript   = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`
	fencingScript = `return redis.call("incr", KEYS[1])`
)

// nativeConditionals reports whether the server supports DELEX/SET ... IFEQ.
// DELEX arrived in Redis 8.4; anything older (and most managed offerings today)
// falls back to the scripts.
func (l *Locker) detectNative(ctx context.Context) bool {
	// COMMAND INFO on an unknown command returns a nil entry, not an error, so the
	// reply has to be inspected rather than the error.
	reply, err := l.client.Do(ctx, "COMMAND", "INFO", "delex").Result()
	if err != nil {
		return false
	}

	// Shape on a real server: [[ "delex", arity, flags, ... ]], or [nil] when the
	// command is unknown. Some test servers ignore the argument and answer with the
	// whole command table, so the name inside the entry decides, not its presence.
	entries, ok := reply.([]any)
	if !ok || len(entries) != 1 {
		return false
	}
	info, ok := entries[0].([]any)
	if !ok || len(info) == 0 {
		return false
	}
	name, ok := info[0].(string)
	return ok && strings.EqualFold(name, "delex")
}

// Locker implements gcache.Locker on Redis.
type Locker struct {
	client     goredis.UniversalClient
	fencingKey string
	native     atomic.Bool
	probed     atomic.Bool
}

// NewLocker creates a locker. Fencing tokens are allocated from `fencingKey`
// (incremented with INCR) so a protected resource can reject a stale holder.
func NewLocker(client goredis.UniversalClient, fencingKey string) (*Locker, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gcache.ErrInvalidConfig)
	}
	if fencingKey == "" {
		fencingKey = "gcache:fencing"
	}
	return &Locker{client: client, fencingKey: fencingKey}, nil
}

// UsesNativeConditionals reports whether the locker picked the Lua-free path. It
// probes on first use, so the answer is only meaningful after a lock call.
func (l *Locker) UsesNativeConditionals() bool { return l.native.Load() }

func (l *Locker) ensureProbe(ctx context.Context) {
	if l.probed.Swap(true) {
		return
	}
	l.native.Store(l.detectNative(ctx))
}

// TryAcquire implements gcache.Locker.
func (l *Locker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (gcache.Lock, bool, error) {
	l.ensureProbe(ctx)

	if ttl <= 0 {
		return nil, false, fmt.Errorf("%w: lock TTL must be positive", gcache.ErrInvalidConfig)
	}
	token, err := newToken()
	if err != nil {
		return nil, false, err
	}

	// SET NX PX is atomic on every supported version.
	ok, err := l.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	if !ok {
		return nil, false, nil
	}

	fencing, err := l.client.Incr(ctx, l.fencingKey).Result()
	if err != nil {
		// The lock is held but we cannot mint a fencing token; release it rather
		// than hand back a lock whose ownership cannot be proven.
		_ = l.releaseToken(ctx, key, token)
		return nil, false, fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}

	return &lock{
		locker:    l,
		key:       key,
		token:     token,
		fencing:   uint64(fencing),
		expiresAt: time.Now().Add(ttl),
	}, true, nil
}

// Acquire implements gcache.Locker, retrying with bounded backoff.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration, wait gcache.WaitPolicy) (gcache.Lock, error) {
	wait.Normalize()
	deadline := time.Now().Add(wait.Timeout)
	backoff := wait.MinBackoff

	for {
		lock, ok, err := l.TryAcquire(ctx, key, ttl)
		if err != nil {
			return nil, err
		}
		if ok {
			return lock, nil
		}
		if time.Now().After(deadline) {
			return nil, gcache.ErrLockNotAcquired
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff *= 2; backoff > wait.MaxBackoff {
			backoff = wait.MaxBackoff
		}
	}
}

// releaseToken deletes the key only if it still holds our token.
func (l *Locker) releaseToken(ctx context.Context, key, token string) error {
	if l.native.Load() {
		// DELEX key IFEQ token — atomic compare-and-delete, Redis 8.4+.
		reply, err := l.client.Do(ctx, "DELEX", key, "IFEQ", token).Result()
		if err != nil {
			return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
		}
		if asInt(reply) == 0 {
			return gcache.ErrLockLost
		}
		return nil
	}

	removed, err := l.client.Eval(ctx, releaseScript, []string{key}, token).Int64()
	if err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	if removed == 0 {
		return gcache.ErrLockLost
	}
	return nil
}

// renewToken extends the TTL only if the token still matches.
func (l *Locker) renewToken(ctx context.Context, key, token string, ttl time.Duration) error {
	if l.native.Load() {
		// SET key token IFEQ token PX ttl: rewrites only when the value matches.
		ok, err := l.client.SetArgs(ctx, key, token, goredis.SetArgs{
			Mode: "IFEQ",
			TTL:  ttl,
			Get:  false,
		}).Result()
		if err != nil {
			return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
		}
		if ok == "" {
			return gcache.ErrLockLost
		}
		return nil
	}

	renewed, err := l.client.Eval(ctx, renewScript, []string{key}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	if renewed == 0 {
		return gcache.ErrLockLost
	}
	return nil
}

type lock struct {
	locker    *Locker
	key       string
	token     string
	fencing   uint64
	expiresAt time.Time
	released  atomic.Bool
}

// Token implements gcache.Lock.
func (l *lock) Token() string { return l.token }

// FencingToken implements gcache.Lock.
func (l *lock) FencingToken() uint64 { return l.fencing }

// ExpiresAt implements gcache.Lock.
func (l *lock) ExpiresAt() time.Time { return l.expiresAt }

// Renew implements gcache.Lock.
func (l *lock) Renew(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("%w: lock TTL must be positive", gcache.ErrInvalidConfig)
	}
	if err := l.locker.renewToken(ctx, l.key, l.token, ttl); err != nil {
		return err
	}
	l.expiresAt = time.Now().Add(ttl)
	return nil
}

// Unlock implements gcache.Lock. It is safe to call once; a second call reports
// ErrLockLost because the token no longer matches.
func (l *lock) Unlock(ctx context.Context) error {
	if l.released.Swap(true) {
		return gcache.ErrLockLost
	}
	return l.locker.releaseToken(ctx, l.key, l.token)
}

func newToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("gcache/redis: generate lock token: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// asInt normalises the integer-shaped replies of Do (int64, string or nil).
func asInt(reply any) int64 {
	switch v := reply.(type) {
	case int64:
		return v
	case string:
		var out int64
		_, _ = fmt.Sscanf(v, "%d", &out)
		return out
	case nil:
		return 0
	default:
		return 0
	}
}
