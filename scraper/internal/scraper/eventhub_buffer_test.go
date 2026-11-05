package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"azure-metrics-exporter/internal/metricdata"
)

func TestEventHubBatchSinkMergesJobs(t *testing.T) {
	sender := &recordingSender{}
	first, second := eventTestBatch(), eventTestBatch()
	second.Job, second.Query = "other", "other_query"
	second.Window.Metrics[0].Name = "other_metric"
	for i := range second.Window.Metrics[0].Samples {
		second.Window.Metrics[0].Samples[i].Timestamp = second.Window.Metrics[0].Samples[i].Timestamp.Add(time.Second)
	}
	if err := eventHubBatchSink(sender, quietLogger())(context.Background(), []Batch{first, second}); err != nil {
		t.Fatal(err)
	}
	if len(sender.bodies) != 1 {
		t.Fatalf("expected one merged event, got %d", len(sender.bodies))
	}
	var got exportWindow
	if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Metrics) != 2 || got.Metrics[0].PromQL != first.Query ||
		got.Metrics[1].PromQL != second.Query ||
		!reflect.DeepEqual(got.Metrics[1].Samples, second.Window.Metrics[0].Samples) ||
		!reflect.DeepEqual(got.Metrics[1].Labels, second.Window.Metrics[0].Labels) ||
		!got.WindowStart.Equal(first.Window.Metrics[0].Samples[0].Timestamp) ||
		!got.WindowEnd.Equal(second.Window.Metrics[0].Samples[1].Timestamp) ||
		bytes.Contains(sender.bodies[0], []byte(`"taskId"`)) {
		t.Fatalf("merged data or metadata incorrect: %s", sender.bodies[0])
	}
}

func TestMergeWindowsCompatibility(t *testing.T) {
	base, err := exportWindows(eventTestBatch())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*exportWindow)
	}{
		{"workspace", func(w *exportWindow) { w.Workspace = "other" }},
		{"cluster", func(w *exportWindow) { w.Cluster = "other" }},
		{"region", func(w *exportWindow) { w.Region = "other" }},
		{"interval", func(w *exportWindow) { w.ScrapeInterval = "15s" }},
		{"source", func(w *exportWindow) { w.Source = "other" }},
		{"schema", func(w *exportWindow) { w.SchemaVersion = "other" }},
		{"resource", func(w *exportWindow) { w.ResourceID = "other" }},
		{"query", func(w *exportWindow) {
			w.Query = &exportQuery{Path: "/probe/metrics", Params: map[string][]string{"metric": {"other"}}}
		}},
		{"partition", func(w *exportWindow) { w.partitionKey = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := base[0]
			test.change(&other)
			got := mergeWindows([]exportWindow{base[0], other})
			if len(got) != 2 || len(got[0].Metrics) != 1 || len(got[1].Metrics) != 1 {
				t.Fatalf("incompatible windows merged: %+v", got)
			}
		})
	}
	platform, err := exportWindows(platformTestBatch())
	if err != nil {
		t.Fatal(err)
	}
	other := platform[0]
	other.ResourceID = strings.ToUpper(other.ResourceID)
	if got := mergeWindows([]exportWindow{platform[0], other}); len(got) != 1 || len(got[0].Metrics) != 2 {
		t.Fatal("compatible platform windows did not merge")
	}
	other.Query = &exportQuery{Path: other.Query.Path, Params: map[string][]string{"metric": {"different"}}}
	if got := mergeWindows([]exportWindow{platform[0], other}); len(got) != 2 {
		t.Fatal("different platform queries merged")
	}
}

func TestEventHubBatchSinkPacksMergedJobs(t *testing.T) {
	sender := &recordingSender{}
	var batches []Batch
	for range 5 {
		batch := eventTestBatch()
		batch.Window.Metrics[0].Labels["large"] = strings.Repeat("x", 50*1024)
		batches = append(batches, batch)
	}
	if err := eventHubBatchSink(sender, quietLogger())(context.Background(), batches); err != nil {
		t.Fatal(err)
	}
	if len(sender.bodies) != 2 {
		t.Fatalf("expected 2 events for 5 merged jobs, got %d", len(sender.bodies))
	}
	total := 0
	for _, body := range sender.bodies {
		var got exportWindow
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		total += len(got.Metrics)
		if len(body) > eventBodyLimit {
			t.Fatalf("merged event exceeds 200 KiB: %d", len(body))
		}
	}
	if total != 5 || sender.keys[0] != sender.keys[1] {
		t.Fatal("packing lost metrics or changed the partition key")
	}
}

func TestEventHubBatchSinkReturnsDeliveryFailure(t *testing.T) {
	sink := eventHubBatchSink(&recordingSender{failAt: 1}, quietLogger())
	if err := sink(context.Background(), []Batch{eventTestBatch(), eventTestBatch()}); err == nil ||
		!strings.Contains(err.Error(), "0/1 acknowledged") {
		t.Fatalf("delivery failure not propagated: %v", err)
	}
}

func TestEventHubBatchSinkRejectsInvalidBatchWithoutPoisoningOthers(t *testing.T) {
	sender := &recordingSender{}
	var logs bytes.Buffer
	sink := eventHubBatchSink(sender, slog.New(slog.NewTextHandler(&logs, nil)))
	bad := eventTestBatch()
	bad.Query = strings.Repeat("x", eventBodyLimit)
	if err := sink(context.Background(), []Batch{bad, eventTestBatch()}); err == nil ||
		len(sender.bodies) != 1 || !strings.Contains(logs.String(), "invalid Event Hub batch") {
		t.Fatalf("invalid batch was not reported or poisoned valid data: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink(ctx, []Batch{eventTestBatch()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not returned: %v", err)
	}
	if len(sender.bodies) != 1 {
		t.Fatal("cancelled batch was sent")
	}
}

func TestPackedWindowsUseCapacityInsteadOfHalving(t *testing.T) {
	windows, err := exportWindows(eventTestBatch())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("largest fitting sample prefix", func(t *testing.T) {
		windows, err := exportWindows(eventTestBatch())
		if err != nil {
			t.Fatal(err)
		}
		window := windows[0]
		original := window.Metrics[0].Samples
		window.Metrics[0].Samples = append(append(append([]metricdata.Sample{}, original...), original...), original...)
		metric := window.Metrics[0]
		window.Metrics[0].Samples = metric.Samples[:4]
		four, err := json.Marshal(window)
		if err != nil {
			t.Fatal(err)
		}
		window.Metrics[0] = metric
		bodies, err := splitWindow(window, len(four))
		if err != nil || len(bodies) != 2 {
			t.Fatalf("sample packing failed: events=%d, error=%v", len(bodies), err)
		}
		for i, body := range bodies {
			var got exportWindow
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
			}
			want := 4
			if i == 1 {
				want = 2
			}
			if len(body) > len(four) || len(got.Metrics) != 1 || len(got.Metrics[0].Samples) != want {
				t.Fatalf("sample prefix not maximal: event=%d bytes=%d", i, len(body))
			}
		}
	})
	window := windows[0]
	metric := window.Metrics[0]
	metric.Labels = map[string]string{"large": strings.Repeat("x", 50*1024)}
	window.Metrics = []exportMetric{metric, metric, metric, metric, metric}
	bodies, err := splitWindow(window, eventBodyLimit)
	if err != nil || len(bodies) != 2 {
		t.Fatalf("expected 2 packed events, got %d: %v", len(bodies), err)
	}
	for i, body := range bodies {
		var got exportWindow
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		want := 3
		if i == 1 {
			want = 2
		}
		if len(body) > eventBodyLimit || len(got.Metrics) != want {
			t.Fatalf("event %d not packed as expected: bytes=%d metrics=%d", i, len(body), len(got.Metrics))
		}
	}
	one := window
	one.Metrics = []exportMetric{metric}
	body, err := json.Marshal(one)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := splitWindow(one, len(body))
	if err != nil || len(exact) != 1 || len(exact[0]) != len(body) {
		t.Fatalf("exact limit rejected: %v", err)
	}
	one.Metrics[0].Samples = one.Metrics[0].Samples[:1]
	single, err := json.Marshal(one)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := splitWindow(one, len(single)-1); err == nil {
		t.Fatal("oversized single sample accepted")
	}
}
