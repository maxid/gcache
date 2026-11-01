// Package redis implements gcache's ports on Redis.
//
// Several processes commonly share one Redis (a two-level cache elsewhere in the
// deployment is local L1 + Redis L2 + pub/sub invalidation), so the key layout and
// the lock scripts here are deliberately the conventional ones: the point is to be
// interchangeable, not inventive.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
)

// Store is a gcache.RemoteStore backed by a Redis client.
type Store struct {
	client goredis.UniversalClient
}

// NewStore wraps a client. The caller owns the client's lifetime unless Close is
// expected to release it.
func NewStore(client goredis.UniversalClient) (*Store, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gcache.ErrInvalidConfig)
	}
	return &Store{client: client}, nil
}

// Get implements gcache.RemoteStore.
func (s *Store) Get(ctx context.Context, key string) (gcache.Entry, bool, error) {
	value, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return gcache.Entry{}, false, nil
	}
	if err != nil {
		return gcache.Entry{}, false, fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}

	entry := gcache.Entry{Value: value}
	if ttl, err := s.client.PTTL(ctx, key).Result(); err == nil && ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl)
	}
	return entry, true, nil
}

// Set implements gcache.RemoteStore.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return s.client.Set(ctx, key, value, 0).Err()
	}
	return s.client.Set(ctx, key, value, ttl).Err()
}

// Delete implements gcache.RemoteStore.
func (s *Store) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// Close implements gcache.RemoteStore.
func (s *Store) Close() error { return s.client.Close() }

// Ping implements gcache.HealthChecker.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}
