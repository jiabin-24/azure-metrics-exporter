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
	"testing/synctest"
	"time"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

func TestEventHubBufferMergesJobsWithinFixedWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		buffer := newEventHubBuffer(sender, quietLogger(), 5*time.Second)
		defer func() {
			if err := buffer.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		first, second := eventTestBatch(), eventTestBatch()
		second.Job, second.Query = "other", "other_query"
		second.Window.Metrics[0].Name = "other_metric"
		for i := range second.Window.Metrics[0].Samples {
			second.Window.Metrics[0].Samples[i].Timestamp = second.Window.Metrics[0].Samples[i].Timestamp.Add(time.Second)
		}
		results := make(chan error, 2)
		go func() { results <- buffer.Send(context.Background(), first) }()
		synctest.Wait()
		time.Sleep(4 * time.Second)
		go func() { results <- buffer.Send(context.Background(), second) }()
		synctest.Wait()
		if len(sender.bodies) != 0 || len(results) != 0 {
			t.Fatal("sent or acknowledged data before the aggregation window elapsed")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		for range 2 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
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
		go func() { results <- buffer.Send(context.Background(), first) }()
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if err := <-results; err != nil || len(sender.bodies) != 2 {
			t.Fatalf("subsequent window not flushed: %v", err)
		}
	})
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

func TestEventHubBufferFlushesAcceptedDataOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		buffer := newEventHubBuffer(sender, quietLogger(), time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- buffer.Send(ctx, eventTestBatch()) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation not returned: %v", err)
		}
		if err := buffer.Close(context.Background()); err != nil || len(sender.bodies) != 1 {
			t.Fatalf("shutdown lost accepted data: %v, events=%d", err, len(sender.bodies))
		}
		if err := buffer.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := buffer.Send(context.Background(), eventTestBatch()); err == nil {
			t.Fatal("closed buffer accepted data")
		}
	})
}

func TestEventHubBufferPacksMergedJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		buffer := newEventHubBuffer(sender, quietLogger(), time.Second)
		defer buffer.Close(context.Background())
		results := make(chan error, 5)
		for range 5 {
			batch := eventTestBatch()
			batch.Window.Metrics[0].Labels["large"] = strings.Repeat("x", 50*1024)
			go func() { results <- buffer.Send(context.Background(), batch) }()
		}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		for range 5 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
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
	})
}

func TestEventHubBufferCloseReportsDeliveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buffer := newEventHubBuffer(&recordingSender{failAt: 1}, quietLogger(), time.Hour)
		result := make(chan error, 1)
		go func() { result <- buffer.Send(context.Background(), eventTestBatch()) }()
		synctest.Wait()
		if err := buffer.Close(context.Background()); err == nil {
			t.Fatal("shutdown delivery failure reported as success")
		}
		if err := <-result; err == nil {
			t.Fatal("shutdown delivery failure did not reach the waiting job")
		}
	})
}

func TestEventHubBufferDeliveryErrorsReachAllJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		sender := &recordingSender{failAt: 1}
		buffer := newEventHubBuffer(sender, slog.New(slog.NewTextHandler(&logs, nil)), time.Second)
		defer buffer.Close(context.Background())
		results := make(chan error, 2)
		for range 2 {
			go func() { results <- buffer.Send(context.Background(), eventTestBatch()) }()
		}
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		for range 2 {
			if err := <-results; err == nil || !strings.Contains(err.Error(), "0/1 acknowledged") {
				t.Fatalf("delivery failure not propagated: %v", err)
			}
		}
		if !strings.Contains(logs.String(), "buffered Event Hub delivery failed") ||
			strings.Contains(logs.String(), "delivery completed") {
			t.Fatalf("failure not logged correctly: %s", &logs)
		}
	})
}

func TestEventHubBufferRejectsInvalidBatchWithoutPoisoningOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		buffer := newEventHubBuffer(sender, quietLogger(), time.Second)
		defer buffer.Close(context.Background())
		bad := eventTestBatch()
		bad.Query = strings.Repeat("x", eventBodyLimit)
		if err := buffer.Send(context.Background(), bad); err == nil {
			t.Fatal("oversized sample accepted")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := buffer.Send(ctx, eventTestBatch()); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled batch accepted: %v", err)
		}
		result := make(chan error, 1)
		go func() { result <- buffer.Send(context.Background(), eventTestBatch()) }()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if err := <-result; err != nil || len(sender.bodies) != 1 {
			t.Fatalf("healthy batch affected by rejection: %v", err)
		}
	})
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
