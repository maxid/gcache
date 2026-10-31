// Package memory provides in-process implementations of gcache's ports, for tests
// and single-process deployments.
//
// It does NOT pretend to be distributed: two processes using it share nothing.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/maxid/gcache"
)

// Store is an in-memory gcache.RemoteStore.
type Store struct {
	mu    sync.Mutex
	items map[string]item
	now   func() time.Time
}

type item struct {
	value     []byte
	expiresAt time.Time
}

// NewStore creates an empty store.
func NewStore() *Store { return &Store{items: map[string]item{}, now: time.Now} }

// Get implements gcache.RemoteStore.
func (s *Store) Get(_ context.Context, key string) (gcache.Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return gcache.Entry{}, false, nil
	}
	if !entry.expiresAt.IsZero() && !s.now().Before(entry.expiresAt) {
		delete(s.items, key)
		return gcache.Entry{}, false, nil
	}
	value := make([]byte, len(entry.value))
	copy(value, entry.value)
	return gcache.Entry{Value: value, ExpiresAt: entry.expiresAt}, true, nil
}

// Set implements gcache.RemoteStore.
func (s *Store) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]byte, len(value))
	copy(copied, value)
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = s.now().Add(ttl)
	}
	s.items[key] = item{value: copied, expiresAt: expiresAt}
	return nil
}

// Delete implements gcache.RemoteStore.
func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
	return nil
}

// Close implements gcache.RemoteStore.
func (s *Store) Close() error { return nil }

// Invalidator is an in-process broadcast bus. Subscribers in the same process see
// messages; other processes do not.
type Invalidator struct {
	mu          sync.RWMutex
	subscribers []chan gcache.Invalidation
	buffer      int
}

// NewInvalidator creates a bus with the given per-subscriber buffer.
func NewInvalidator(buffer int) *Invalidator {
	if buffer <= 0 {
		buffer = 64
	}
	return &Invalidator{buffer: buffer}
}

// Publish implements gcache.Invalidator.
func (i *Invalidator) Publish(_ context.Context, message gcache.Invalidation) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, subscriber := range i.subscribers {
		select {
		case subscriber <- message:
		default:
			// A full subscriber is dropped, exactly like a lost pub/sub message:
			// TTL is the backstop, and the caller is told nothing.
		}
	}
	return nil
}

// Subscribe implements gcache.Invalidator.
func (i *Invalidator) Subscribe(ctx context.Context, handler func(gcache.Invalidation)) error {
	ch := make(chan gcache.Invalidation, i.buffer)

	i.mu.Lock()
	i.subscribers = append(i.subscribers, ch)
	i.mu.Unlock()

	defer func() {
		i.mu.Lock()
		for idx, subscriber := range i.subscribers {
			if subscriber == ch {
				i.subscribers = append(i.subscribers[:idx], i.subscribers[idx+1:]...)
				break
			}
		}
		i.mu.Unlock()
		close(ch)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-ch:
			handler(message)
		}
	}
}

// Close implements gcache.Invalidator.
func (i *Invalidator) Close() error { return nil }
