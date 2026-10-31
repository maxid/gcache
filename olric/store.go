// Package olric implements gcache's shared tier on an embedded Olric cluster.
//
// Olric is the closest Go equivalent of Hazelcast: a clustered distributed map. It
// replaces the Redis L2 when a deployment should not depend on Redis at all; the
// local tier and the invalidation contract are unchanged.
//
// The package is split in two on purpose:
//
//   - DMap, the narrow port gcache needs (plain bytes plus a TTL), which keeps the
//     cache free of Olric types and makes the adapter testable;
//   - FromDMap, the thin bridge to olric.DMap, the only place that knows Olric's
//     response and option types.
package olric

import (
	"context"
	"fmt"
	"time"

	olricdb "github.com/olric-data/olric"

	"github.com/maxid/gcache"
)

// DMap is the subset of a distributed map that a shared tier needs.
type DMap interface {
	// Get returns the value and whether the key exists. A missing key is
	// (nil, false, nil); an outage is an error.
	Get(ctx context.Context, key string) ([]byte, bool, error)
	// Put stores the value for at most ttl.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes the key; deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// Store is a gcache.RemoteStore over a DMap.
type Store struct {
	dmap       DMap
	defaultTTL time.Duration
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithDefaultTTL sets the TTL used when a write carries none.
func WithDefaultTTL(ttl time.Duration) StoreOption {
	return func(s *Store) {
		if ttl > 0 {
			s.defaultTTL = ttl
		}
	}
}

// NewStore wraps a DMap.
func NewStore(dmap DMap, opts ...StoreOption) (*Store, error) {
	if dmap == nil {
		return nil, fmt.Errorf("%w: DMap is required", gcache.ErrInvalidConfig)
	}
	s := &Store{dmap: dmap, defaultTTL: time.Hour}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Get implements gcache.RemoteStore.
func (s *Store) Get(ctx context.Context, key string) (gcache.Entry, bool, error) {
	value, ok, err := s.dmap.Get(ctx, key)
	if err != nil {
		return gcache.Entry{}, false, fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	if !ok {
		return gcache.Entry{}, false, nil
	}
	return gcache.Entry{Value: value}, true, nil
}

// Set implements gcache.RemoteStore.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	if err := s.dmap.Put(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}

// Delete implements gcache.RemoteStore.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.dmap.Delete(ctx, key); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}

// Close implements gcache.RemoteStore. The Olric node belongs to the caller.
func (s *Store) Close() error { return nil }

// FromDMap adapts a real olric.DMap to the port. This is the only place that
// touches Olric's response, option and error types.
func FromDMap(dmap olricdb.DMap) DMap { return olricBridge{dmap: dmap} }

type olricBridge struct{ dmap olricdb.DMap }

func (b olricBridge) Get(ctx context.Context, key string) ([]byte, bool, error) {
	response, err := b.dmap.Get(ctx, key)
	if err != nil {
		if err == olricdb.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	if response == nil {
		return nil, false, nil
	}
	value, err := response.Byte()
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (b olricBridge) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return b.dmap.Put(ctx, key, value, olricdb.EX(ttl))
}

func (b olricBridge) Delete(ctx context.Context, key string) error {
	_, err := b.dmap.Delete(ctx, key)
	return err
}
