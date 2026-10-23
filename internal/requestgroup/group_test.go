package requestgroup

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDoSupportsTypedResultsAndContextValues(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "request-value")
	var numbers Group[int]
	value, err := numbers.Do(ctx, "count", time.Second, func(ctx context.Context) (int, error) {
		if ctx.Value(contextKey{}) != "request-value" {
			return 0, errors.New("shared request lost context values")
		}
		return 42, nil
	})
	if err != nil || value != 42 {
		t.Fatalf("typed integer result changed: value=%d err=%v", value, err)
	}

	var properties Group[map[string]string]
	result, err := properties.Do(ctx, "resource:/example", time.Second, func(context.Context) (map[string]string, error) {
		return map[string]string{"location": "australiaeast", "environment": "production"}, nil
	})
	if err != nil || result["location"] != "australiaeast" || result["environment"] != "production" {
		t.Fatalf("typed property result changed: result=%v err=%v", result, err)
	}
}

func TestDoSeparatesOperationKeys(t *testing.T) {
	var group Group[string]
	started := make(chan string, 3)
	results := make(chan error, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	for _, key := range []string{"region:/example", "tags:/example", "REGION:/example"} {
		go func() {
			value, err := group.Do(t.Context(), key, time.Second, func(ctx context.Context) (string, error) {
				started <- key
				select {
				case <-release:
					return key, nil
				case <-ctx.Done():
					return "", ctx.Err()
				}
			})
			if err == nil && value != key {
				err = errors.New("different operation keys shared a result")
			}
			results <- err
		}()
	}
	keys := make(map[string]bool)
	for range 3 {
		select {
		case key := <-started:
			keys[key] = true
		case <-time.After(5 * time.Second):
			t.Fatal("different keys did not start concurrently")
		}
	}
	if len(keys) != 3 {
		t.Fatalf("request group must not normalize keys or merge different operations: %v", keys)
	}
	unblock()
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDoSharedTimeoutDoesNotCacheFailure(t *testing.T) {
	var group Group[string]
	calls := 0
	for range 2 {
		value, err := group.Do(t.Context(), "resource", 20*time.Millisecond, func(ctx context.Context) (string, error) {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 20*time.Millisecond {
				return "", errors.New("shared request has no bounded deadline")
			}
			<-ctx.Done()
			return "", ctx.Err()
		})
		if value != "" || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected shared request timeout, got value=%q err=%v", value, err)
		}
	}
	if calls != 2 {
		t.Fatalf("completed failures must not be cached, got %d calls", calls)
	}
}

func TestTryDoSkipsInFlightRequestsAndAllowsRetry(t *testing.T) {
	for _, starter := range []string{"Do", "TryDo"} {
		t.Run(starter, func(t *testing.T) {
			var group Group[int]
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			type result struct {
				value int
				err   error
			}
			results := make(chan result, 1)
			do := group.Do
			if starter == "TryDo" {
				do = group.TryDo
			}
			go func() {
				value, err := do(t.Context(), "resource", time.Second, func(ctx context.Context) (int, error) {
					started <- struct{}{}
					select {
					case <-release:
						return 42, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				})
				results <- result{value, err}
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("initial request did not start")
			}
			for range 17 {
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				value, err := group.TryDo(ctx, "resource", time.Second, func(context.Context) (int, error) {
					t.Error("skipped request executed its callback")
					return 99, nil
				})
				cancel()
				if value != 0 || !errors.Is(err, ErrInFlight) {
					t.Fatalf("busy key must return ErrInFlight without waiting: value=%d err=%v", value, err)
				}
			}
			unblock()
			got := <-results
			if got.value != 42 || got.err != nil {
				t.Fatalf("skipped requests affected the original request: %+v", got)
			}
			value, err := group.TryDo(t.Context(), "resource", time.Second, func(context.Context) (int, error) {
				return 99, nil
			})
			if value != 99 || err != nil {
				t.Fatalf("completed key was not released for a new request: value=%d err=%v", value, err)
			}
		})
	}
}

func TestTryDoValidatesTimeoutAndCancellation(t *testing.T) {
	var group Group[int]
	fn := func(context.Context) (int, error) {
		t.Error("invalid or cancelled request executed its callback")
		return 42, nil
	}
	for _, timeout := range []time.Duration{0, -time.Second} {
		if _, err := group.TryDo(t.Context(), "resource", timeout, fn); err == nil {
			t.Fatal("non-positive timeout must fail")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := group.TryDo(ctx, "resource", time.Second, fn); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request must not start: %v", err)
	}
}

func TestDoRejectsNonPositiveTimeout(t *testing.T) {
	var group Group[int]
	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := group.Do(t.Context(), "resource", timeout, func(context.Context) (int, error) {
			t.Error("invalid timeout started an underlying request")
			return 42, nil
		})
		if err == nil {
			t.Fatalf("timeout %v must return an error", timeout)
		}
	}
}
