package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
)

// Invalidator broadcasts invalidation messages over a Redis pub/sub channel.
//
// Delivery is best effort, exactly like Redisson's RTopic: a subscriber that is
// disconnected misses messages, which is why every entry also carries a TTL.
type Invalidator struct {
	client  goredis.UniversalClient
	channel string
}

// NewInvalidator creates a broadcaster on `channel`.
func NewInvalidator(client goredis.UniversalClient, channel string) (*Invalidator, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gcache.ErrInvalidConfig)
	}
	if channel == "" {
		return nil, fmt.Errorf("%w: channel is required", gcache.ErrInvalidConfig)
	}
	return &Invalidator{client: client, channel: channel}, nil
}

// Publish implements gcache.Invalidator.
func (i *Invalidator) Publish(ctx context.Context, message gcache.Invalidation) error {
	raw, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("gcache/redis: encode invalidation: %w", err)
	}
	if err := i.client.Publish(ctx, i.channel, raw).Err(); err != nil {
		return fmt.Errorf("%w: %v", gcache.ErrUnavailable, err)
	}
	return nil
}

// Subscribe implements gcache.Invalidator. A malformed message is skipped: one bad
// publisher must not stop the subscriber from draining the rest.
//
// The messages arrive on a channel and cancellation is observed by a select. The
// blocking ReceiveMessage form does not return on context cancellation while the
// socket read is in flight, which made Close() hang forever.
func (i *Invalidator) Subscribe(ctx context.Context, handler func(gcache.Invalidation)) error {
	pubsub := i.client.Subscribe(ctx, i.channel)
	defer func() { _ = pubsub.Close() }()

	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-messages:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("%w: subscription closed", gcache.ErrUnavailable)
			}
			var invalidation gcache.Invalidation
			if err := json.Unmarshal([]byte(message.Payload), &invalidation); err != nil {
				continue
			}
			handler(invalidation)
		}
	}
}

// Close implements gcache.Invalidator.
func (i *Invalidator) Close() error { return nil }
