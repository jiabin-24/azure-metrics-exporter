package scraper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
)

func eventHubBatchSink(sender eventSender, logger *slog.Logger) BatchSink {
	return func(ctx context.Context, batches []Batch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var windows []exportWindow
		var validationErr error
		for _, batch := range batches {
			batchWindows, err := windowsForExport(batch, logger)
			if err == nil {
				for _, window := range batchWindows {
					if _, err = splitWindow(window, eventBodyLimit); err != nil {
						break
					}
				}
			}
			if err != nil {
				logger.Error("invalid Event Hub batch", "job", batch.Job, "task_id", batch.TaskID, "error", err)
				validationErr = errors.Join(validationErr, fmt.Errorf("job %q: %w", batch.Job, err))
				continue
			}
			windows = append(windows, batchWindows...)
		}
		return errors.Join(validationErr, sendWindows(ctx, sender, logger, mergeWindows(windows)))
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
