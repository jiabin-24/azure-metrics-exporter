package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"
)

type windowRequest struct {
	windows []exportWindow
	result  chan error
}

type eventHubBuffer struct {
	requests  chan windowRequest
	stop      chan context.Context
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	logger    *slog.Logger
}

func newEventHubBuffer(sender eventSender, logger *slog.Logger, interval time.Duration) *eventHubBuffer {
	buffer := &eventHubBuffer{
		requests: make(chan windowRequest), stop: make(chan context.Context, 1),
		done: make(chan struct{}), logger: logger,
	}
	go buffer.run(sender, interval)
	return buffer
}

func (b *eventHubBuffer) Send(ctx context.Context, batch Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	windows, err := windowsForExport(batch, b.logger)
	if err != nil {
		return err
	}
	if len(windows) == 0 {
		return nil
	}
	// Reject an invalid batch before it can poison other jobs in the buffer.
	for _, window := range windows {
		if _, err := splitWindow(window, eventBodyLimit); err != nil {
			return err
		}
	}
	request := windowRequest{windows: windows, result: make(chan error, 1)}
	select {
	case <-b.done:
		return fmt.Errorf("event hub buffer is closed")
	case <-ctx.Done():
		return ctx.Err()
	case b.requests <- request:
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		// Accepted data remains queued for flushing, including during shutdown.
		return ctx.Err()
	}
}

func (b *eventHubBuffer) Close(ctx context.Context) error {
	b.closeOnce.Do(func() { b.stop <- ctx })
	select {
	case <-b.done:
		return b.closeErr
	case <-ctx.Done():
		return fmt.Errorf("flush Event Hub buffer: %w", ctx.Err())
	}
}

func (b *eventHubBuffer) run(sender eventSender, interval time.Duration) {
	defer close(b.done)
	var pending []windowRequest
	var timer *time.Timer
	var tick <-chan time.Time
	flush := func(ctx context.Context) error {
		var windows []exportWindow
		for _, request := range pending {
			windows = append(windows, request.windows...)
		}
		err := sendWindows(ctx, sender, b.logger, mergeWindows(windows))
		if err != nil {
			b.logger.Error("buffered Event Hub delivery failed", "batches", len(pending), "error", err)
		}
		for _, request := range pending {
			request.result <- err
		}
		pending = nil
		return err
	}
	for {
		select {
		case request := <-b.requests:
			pending = append(pending, request)
			if timer == nil {
				timer = time.NewTimer(interval)
				tick = timer.C
			}
		case <-tick:
			// flush logs errors and returns them to the pending requests.
			_ = flush(context.Background())
			timer, tick = nil, nil
		case ctx := <-b.stop:
			if timer != nil {
				timer.Stop()
			}
			b.closeErr = flush(ctx)
			return
		}
	}
}

func mergeWindows(windows []exportWindow) []exportWindow {
	type key struct {
		schema, source, workspace, cluster, region, interval string
		resource, queryPath, queryParams, partition          string
	}
	groups := make(map[key]int)
	var merged []exportWindow
	for _, window := range windows {
		group := key{
			schema: window.SchemaVersion, source: window.Source, workspace: window.Workspace,
			cluster: window.Cluster, region: window.Region, interval: window.ScrapeInterval,
			resource: strings.ToLower(window.ResourceID), partition: window.partitionKey,
		}
		if window.Query != nil {
			group.queryPath = window.Query.Path
			group.queryParams = url.Values(window.Query.Params).Encode()
		}
		if index, exists := groups[group]; exists {
			target := &merged[index]
			target.Metrics = append(target.Metrics, window.Metrics...)
			if window.WindowStart.Before(target.WindowStart) {
				target.WindowStart = window.WindowStart
			}
			if window.WindowEnd.After(target.WindowEnd) {
				target.WindowEnd = window.WindowEnd
			}
		} else {
			groups[group] = len(merged)
			merged = append(merged, window)
		}
	}
	return merged
}
