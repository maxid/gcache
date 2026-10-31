package gcache

import (
	"context"
	"sync"
)

// group collapses concurrent loads for the same key into one call. Unlike
// x/sync/singleflight, every waiter keeps its own context: a cancelled caller
// leaves without waiting for the shared result, and only the leader's context
// bounds the load.
type group struct {
	mu    sync.Mutex
	calls map[string]*call
}

type call struct {
	done   chan struct{}
	entry  Entry
	err    error
	leader bool
}

func newGroup() *group { return &group{calls: map[string]*call{}} }

// do runs fn once per key while callers overlap.
func (g *group) do(ctx context.Context, key string, fn func(context.Context) (Entry, error)) (Entry, error) {
	g.mu.Lock()
	if existing, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-existing.done:
			return existing.entry, existing.err
		case <-ctx.Done():
			return Entry{}, ctx.Err()
		}
	}

	current := &call{done: make(chan struct{}), leader: true}
	g.calls[key] = current
	g.mu.Unlock()

	go func() {
		defer func() {
			g.mu.Lock()
			delete(g.calls, key)
			g.mu.Unlock()
			close(current.done)
		}()
		current.entry, current.err = fn(ctx)
	}()

	select {
	case <-current.done:
		return current.entry, current.err
	case <-ctx.Done():
		return Entry{}, ctx.Err()
	}
}
