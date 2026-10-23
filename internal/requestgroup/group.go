// Package requestgroup coalesces concurrent requests without caching their results.
package requestgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrInFlight indicates that another request is already running for the same key.
var ErrInFlight = errors.New("request already in flight")

// Group shares one in-flight request per key. Its zero value is ready to use.
// A Group must not be copied after first use.
type Group[T any] struct {
	mu      sync.Mutex
	pending map[string]*call[T]
}

type call[T any] struct {
	done  chan struct{}
	value T
	err   error
}

// Do shares the first caller's fn, timeout and context values for exactly matching keys.
// Each caller can cancel its own wait without cancelling the shared request.
// timeout must be positive, and fn must honor its context deadline.
// Callers must include the operation and scope in the key when requests differ,
// and must not mutate a shared result without synchronization.
func (g *Group[T]) Do(ctx context.Context, key string, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	return g.do(ctx, key, timeout, fn, true)
}

// TryDo behaves like Do, but returns ErrInFlight immediately if the key is busy.
// The caller starting a new request still waits for its result or cancellation.
func (g *Group[T]) TryDo(ctx context.Context, key string, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	return g.do(ctx, key, timeout, fn, false)
}

func (g *Group[T]) do(ctx context.Context, key string, timeout time.Duration, fn func(context.Context) (T, error), wait bool) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if timeout <= 0 {
		return zero, fmt.Errorf("request group timeout must be positive")
	}
	g.mu.Lock()
	if g.pending == nil {
		g.pending = make(map[string]*call[T])
	}
	request := g.pending[key]
	if request != nil && !wait {
		g.mu.Unlock()
		return zero, ErrInFlight
	}
	if request == nil {
		request = &call[T]{done: make(chan struct{})}
		g.pending[key] = request
		requestCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		go func() {
			defer cancel()
			request.value, request.err = fn(requestCtx)
			g.mu.Lock()
			delete(g.pending, key)
			close(request.done)
			g.mu.Unlock()
		}()
	}
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-request.done:
		return request.value, request.err
	}
}
