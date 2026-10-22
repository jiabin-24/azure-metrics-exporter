package scraper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

type recordingSender struct {
	bodies [][]byte
	keys   []string
	failAt int
}

func (s *recordingSender) Send(ctx context.Context, body []byte, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failAt > 0 && len(s.bodies)+1 == s.failAt {
		return errors.New("send rejected")
	}
	s.bodies = append(s.bodies, body)
	s.keys = append(s.keys, key)
	return nil
}

func eventTestBatch() Batch {
	start := time.Date(2026, 10, 21, 15, 0, 26, 123000000, time.UTC)
	return Batch{
		Job: "controlplane", Query: "apiserver_request_total", Interval: time.Minute,
		Workspace: "workspace-a",
		Window: &metricdata.Window{
			SchemaVersion: "1.0", Source: metricdata.PrometheusSource,
			Metrics: []metricdata.Series{
				{Name: "apiserver_request_total", Labels: map[string]string{
					"cluster": "prod", "location": "australiaeast", "code": "200",
				}, Samples: []metricdata.Sample{{Timestamp: start, Value: 1313}, {Timestamp: start.Add(15 * time.Second), Value: 1314}}},
			},
		},
	}
}

func TestEventHubEnvelopeMatchesExampleAndPreservesData(t *testing.T) {
	batch := eventTestBatch()
	sender := &recordingSender{}
	if err := eventHubSink(sender, quietLogger())(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	wantKey := fmt.Sprintf("%x", sha256.Sum256([]byte("workspace-a\x00prod")))
	if len(sender.bodies) != 1 || sender.keys[0] != wantKey {
		t.Fatalf("incorrect events or partition key: %+v", sender)
	}
	var got exportWindow
	if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
		t.Fatal(err)
	}
	series := batch.Window.Metrics[0]
	if got.Source != "aks-controlplane" || got.SchemaVersion != "1.0" ||
		got.Cluster != "prod" || got.Region != "australiaeast" || got.ScrapeInterval != "1m0s" ||
		!got.WindowStart.Equal(series.Samples[0].Timestamp) || !got.WindowEnd.Equal(series.Samples[1].Timestamp) ||
		len(got.Metrics) != 1 || got.Metrics[0].Name != series.Name || got.Metrics[0].PromQL != batch.Query ||
		!reflect.DeepEqual(got.Metrics[0].Samples, series.Samples) || !reflect.DeepEqual(got.Metrics[0].Labels, series.Labels) {
		t.Fatalf("incorrect export: %+v", got)
	}
	example := []byte(`{"schemaVersion":"1.0","source":"aks-controlplane","workspace":"workspace-a","cluster":"aks-ae-cluster-1","region":"australiaeast","windowStart":"2026-10-15T12:24:15Z","windowEnd":"2026-10-15T12:27:15Z","scrapeInterval":"15s","metrics":[{"name":"apiserver_request_rate_by_code","promql":"sum(rate(apiserver_request_total[5m])) by (cluster, code)","labels":{"code":"200"},"samples":[["2026-10-15T12:24:15Z",7610.422923976607]]}]}`)
	var expected, actual map[string]json.RawMessage
	if err := json.Unmarshal(example, &expected); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(sender.bodies[0], &actual); err != nil {
		t.Fatal(err)
	}
	assertSameKeys := func(a, b map[string]json.RawMessage) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("different JSON field count: %v / %v", a, b)
		}
		for key := range a {
			if _, ok := b[key]; !ok {
				t.Fatalf("missing example JSON field %q", key)
			}
		}
	}
	assertSameKeys(expected, actual)
	var expectedMetrics, actualMetrics []map[string]json.RawMessage
	if err := json.Unmarshal(expected["metrics"], &expectedMetrics); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(actual["metrics"], &actualMetrics); err != nil {
		t.Fatal(err)
	}
	assertSameKeys(expectedMetrics[0], actualMetrics[0])
}

func TestExportGroupsAndMetadata(t *testing.T) {
	batch := eventTestBatch()
	other := batch.Window.Metrics[0]
	other.Labels = map[string]string{"cluster": "other", "region": "eastus"}
	batch.Window.Metrics = append(batch.Window.Metrics, other)
	windows, err := exportWindows(batch)
	if err != nil || len(windows) != 2 || windows[0].Cluster != "other" || windows[0].Region != "eastus" {
		t.Fatalf("wrong groups: %+v, %v", windows, err)
	}
	delete(batch.Window.Metrics[0].Labels, "cluster")
	if _, err := exportWindows(batch); err == nil {
		t.Fatal("missing cluster must fail")
	}
	batch.Labels = map[string]string{"cluster": "fallback", "region": "westus"}
	delete(batch.Window.Metrics[0].Labels, "location")
	windows, err = exportWindows(batch)
	if err != nil || windows[0].Cluster != "fallback" || windows[0].Region != "westus" {
		t.Fatalf("static metadata not used: %+v, %v", windows, err)
	}
}

func TestMissingRegionWarnsAndEmptyResultDoesNotSend(t *testing.T) {
	batch := eventTestBatch()
	delete(batch.Window.Metrics[0].Labels, "location")
	var logs bytes.Buffer
	sender := &recordingSender{}
	sink := eventHubSink(sender, slog.New(slog.NewTextHandler(&logs, nil)))
	if err := sink(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "exporting empty region") || !bytes.Contains(sender.bodies[0], []byte(`"region":""`)) {
		t.Fatalf("missing region was not reported: %s", &logs)
	}
	batch.Window.Metrics = []metricdata.Series{}
	if err := sink(context.Background(), batch); err != nil || len(sender.bodies) != 1 ||
		!strings.Contains(logs.String(), "no Event Hub events sent") {
		t.Fatalf("empty result mishandled: %v, %s", err, &logs)
	}
}

func TestSplitWindowsPreservesAllSeriesAndSamples(t *testing.T) {
	for _, manySeries := range []bool{false, true} {
		t.Run(fmt.Sprint(manySeries), func(t *testing.T) {
			batch := eventTestBatch()
			base := batch.Window.Metrics[0]
			start := base.Samples[0].Timestamp
			batch.Window.Metrics = nil
			const count = 12000
			for i := range count {
				sample := metricdata.Sample{Timestamp: start.Add(time.Duration(i) * time.Second), Value: float64(i)}
				if manySeries {
					batch.Window.Metrics = append(batch.Window.Metrics, metricdata.Series{
						Name: base.Name, Labels: map[string]string{"cluster": "prod", "id": fmt.Sprint(i)},
						Samples: []metricdata.Sample{sample},
					})
				} else {
					if i == 0 {
						base.Samples = nil
						batch.Window.Metrics = append(batch.Window.Metrics, base)
					}
					batch.Window.Metrics[0].Samples = append(batch.Window.Metrics[0].Samples, sample)
				}
			}
			sender := &recordingSender{}
			if err := eventHubSink(sender, quietLogger())(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			if len(sender.bodies) < 2 {
				t.Fatal("oversized data was not split")
			}
			next := 0
			for _, body := range sender.bodies {
				if len(body) > eventBodyLimit {
					t.Fatalf("event exceeds actual body limit: %d", len(body))
				}
				var window exportWindow
				if err := json.Unmarshal(body, &window); err != nil {
					t.Fatal(err)
				}
				if window.Cluster != "prod" || !window.WindowStart.Equal(start) {
					t.Fatalf("split changed batch metadata: %+v", window)
				}
				for _, series := range window.Metrics {
					for _, point := range series.Samples {
						if point.Value != float64(next) || !point.Timestamp.Equal(start.Add(time.Duration(next)*time.Second)) {
							t.Fatalf("sample lost, duplicated or modified at %d: %+v", next, point)
						}
						next++
					}
				}
			}
			if next != count {
				t.Fatalf("expected %d samples, got %d", count, next)
			}
		})
	}
}

func TestEventHubFailuresAndCancellation(t *testing.T) {
	batch := eventTestBatch()
	for _, test := range []string{"send", "cancel", "oversized", "invalid"} {
		t.Run(test, func(t *testing.T) {
			sender := &recordingSender{failAt: 1}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			copy := batch
			switch test {
			case "cancel":
				cancel()
			case "oversized":
				copy.Query = strings.Repeat("x", eventBodyLimit)
			case "invalid":
				copy.Window = nil
			}
			var logs bytes.Buffer
			err := eventHubSink(sender, slog.New(slog.NewTextHandler(&logs, nil)))(ctx, copy)
			if err == nil || len(sender.bodies) != 0 || strings.Contains(logs.String(), "delivery completed") {
				t.Fatalf("failure was treated as success: %v, %s", err, &logs)
			}
		})
	}
}

func TestEventHubPartialFailureAndPreflightValidation(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprint(oversized), func(t *testing.T) {
			batch := eventTestBatch()
			second := batch.Window.Metrics[0]
			second.Labels = map[string]string{"cluster": "zzz"}
			if oversized {
				second.Labels["large"] = strings.Repeat("x", eventBodyLimit)
				second.Samples = second.Samples[:1]
			}
			batch.Window.Metrics = append(batch.Window.Metrics, second)
			sender := &recordingSender{failAt: 2}
			err := eventHubSink(sender, quietLogger())(context.Background(), batch)
			if err == nil {
				t.Fatal("failed batch reported success")
			}
			if oversized {
				if len(sender.bodies) != 0 {
					t.Fatal("encoding failure after sending part of the batch")
				}
			} else if len(sender.bodies) != 1 || !strings.Contains(err.Error(), "1/2 acknowledged") {
				t.Fatalf("partial delivery was not reported: %v", err)
			}
		})
	}
}

func TestEventHubSASConnectionValidation(t *testing.T) {
	cfg := EventHubConfig{Namespace: "example.servicebus.windows.net", Name: "metrics"}
	valid := "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=secret==;EntityPath=metrics"
	if err := validateConnectionString(valid, cfg); err != nil {
		t.Fatal(err)
	}

	for _, value := range []string{
		"", strings.Replace(valid, "sb://", "https://", 1),
		strings.Replace(valid, "example.servicebus", "wrong.servicebus", 1),
		strings.Replace(valid, "EntityPath=metrics", "EntityPath=wrong", 1),
		strings.Replace(valid, "SharedAccessKey=secret==;", "", 1),
		valid + ";SharedAccessKey=other", valid + ";Unknown=secret==",
	} {
		t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", value)
		_, _, err := NewEventHubSink(cfg, quietLogger())
		if err == nil || strings.Contains(err.Error(), "secret==") {
			t.Fatalf("invalid credentials accepted or exposed: %v", err)
		}
	}
}

func TestLiveEventHubDelivery(t *testing.T) {
	if os.Getenv("AZURE_EVENTHUB_LIVE_TEST") != "1" {
		t.Skip("set AZURE_EVENTHUB_LIVE_TEST=1 to scrape the local exporter and send real events")
	}
	cfg, err := LoadConfig(filepath.Join("..", "..", "scraper.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventHub == nil {
		t.Fatal("live test requires event_hub configuration")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sink, closeProducer, err := NewEventHubSink(*cfg.EventHub, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := closeProducer(ctx); err != nil {
			t.Error(err)
		}
	}()
	delivered := 0
	s := New(cfg, logger, func(ctx context.Context, batch Batch) error {
		if batch.Window == nil || len(batch.Window.Metrics) == 0 {
			return fmt.Errorf("live scrape returned no metrics; cannot verify delivery")
		}
		if err := sink(ctx, batch); err != nil {
			return err
		}
		delivered++
		return nil
	})
	defer s.Client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, job := range cfg.Jobs {
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				if err := s.Scrape(ctx, job, target, group.Labels); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if delivered == 0 {
		t.Fatal("no live batches delivered")
	}
}

func TestScrapeToEventHubSink(t *testing.T) {
	payload, err := json.Marshal(eventTestBatch().Window)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	job.Params = map[string][]string{"query": {"apiserver_request_total"}, "format": {"json"}}
	sender := &recordingSender{}
	s := New(&Config{}, quietLogger(), eventHubSink(sender, quietLogger()))
	defer s.Client.CloseIdleConnections()
	if err := s.Scrape(context.Background(), job, job.StaticConfigs[0].Targets[0], nil); err != nil {
		t.Fatal(err)
	}
	if len(sender.bodies) != 1 || !bytes.Contains(sender.bodies[0], []byte(`"promql":"apiserver_request_total"`)) {
		t.Fatal("scrape query or data did not reach Event Hub sink")
	}
}

func TestEventHubConfigValidation(t *testing.T) {
	const valid = `
event_hub:
  namespace: example.servicebus.windows.net
  name: metrics
scrape_configs:
- job_name: controlplane
  metrics_path: /probe/metrics/promql
  params:
    query: [up]
    format: [json]
  static_configs:
  - targets: [localhost:8080]
`
	if _, err := ParseConfig([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		strings.Replace(valid, "example.servicebus.windows.net", "https://example.servicebus.windows.net", 1),
		strings.Replace(valid, "name: metrics", "name: ''", 1),
		strings.Replace(valid, "/probe/metrics/promql", "/metrics", 1),
		strings.Replace(valid, "[json]", "[prometheus]", 1),
		strings.Replace(valid, "[up]", "[up, other]", 1),
	} {
		if _, err := ParseConfig([]byte(value)); err == nil {
			t.Fatal("invalid Event Hub job accepted")
		}
	}
}
