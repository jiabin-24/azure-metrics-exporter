package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

const platformResourceID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm"

func TestMixedEventHubJobs(t *testing.T) {
	const yaml = `
event_hub:
  namespace: example.servicebus.windows.net
  name: metrics
scrape_configs:
- job_name: promql
  metrics_path: /probe/metrics/promql
  params:
    format: [json]
    query: [up]
  static_configs:
  - targets: [localhost:8080]
- job_name: platform
  metrics_path: /probe/metrics/resource
  params:
    format: [json]
    metric: [cpu]
  static_configs:
  - targets: [localhost:8080]
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil || len(cfg.Jobs) != 2 {
		t.Fatalf("mixed Event Hub jobs rejected: %v", err)
	}
	for _, platform := range []bool{false, true} {
		batch := eventTestBatch()
		if platform {
			batch = platformTestBatch()
		}
		sender := &recordingSender{failAt: 1}
		err := eventHubSink(sender, quietLogger())(context.Background(), batch)
		if err == nil || !strings.Contains(err.Error(), "0/1 acknowledged") {
			t.Fatalf("source-specific send failure was not reported: platform=%v, error=%v", platform, err)
		}
	}
}

func platformTestBatch() Batch {
	batch := eventTestBatch()
	batch.Query = ""
	batch.MetricsPath = "/probe/metrics/resource"
	batch.Params = map[string][]string{
		"format": {"json"}, "target": {platformResourceID}, "metric": {"Percentage CPU"},
		"aggregation": {"average", "maximum"}, "timespan": {"PT3M"}, "interval": {"PT1M"},
	}
	batch.Window.Source = metricdata.Source
	series := &batch.Window.Metrics[0]
	series.Name = "azure_cpu"
	series.Labels = map[string]string{
		"resourceID": platformResourceID, "subscriptionID": "sub", "aggregation": "average",
		"unit": "Percent", "dimension": "node-a", "region": "eastus",
	}
	return batch
}

func TestPlatformEventHubEnvelope(t *testing.T) {
	batch := platformTestBatch()
	sender := &recordingSender{}
	if err := eventHubSink(sender, quietLogger())(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(sender.bodies) != 1 || len(sender.keys[0]) != 64 {
		t.Fatalf("incorrect platform events or partition key: %+v", sender)
	}
	var got exportWindow
	if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
		t.Fatal(err)
	}
	series := batch.Window.Metrics[0]
	if got.Source != metricdata.Source || got.SchemaVersion != "1.0" || got.ResourceID != platformResourceID ||
		got.Cluster != "" || got.Region != "eastus" || got.ScrapeInterval != batch.Interval.String() ||
		!got.WindowStart.Equal(series.Samples[0].Timestamp) ||
		!got.WindowEnd.Equal(series.Samples[1].Timestamp) ||
		got.Query == nil || got.Query.Path != batch.MetricsPath || !reflect.DeepEqual(got.Query.Params, batch.Params) ||
		len(got.Metrics) != 1 || got.Metrics[0].Name != series.Name ||
		!reflect.DeepEqual(got.Metrics[0].Labels, series.Labels) || !reflect.DeepEqual(got.Metrics[0].Samples, series.Samples) {
		t.Fatalf("platform metadata or samples changed: %+v", got)
	}
	if bytes.Contains(sender.bodies[0], []byte(`"promql"`)) || bytes.Contains(sender.bodies[0], []byte(`"cluster"`)) {
		t.Fatalf("platform event contains fabricated PromQL/cluster fields: %s", sender.bodies[0])
	}
}

func TestPlatformResourceGrouping(t *testing.T) {
	batch := platformTestBatch()
	second := batch.Window.Metrics[0]
	second.Labels = map[string]string{"resourceID": platformResourceID + "2", "aggregation": "maximum"}
	third := second
	third.Labels = map[string]string{"resourceID": strings.ToUpper(platformResourceID), "aggregation": "maximum", "region": "eastus"}
	batch.Window.Metrics = append(batch.Window.Metrics, second, third)
	sender := &recordingSender{}
	if err := eventHubSink(sender, quietLogger())(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(sender.bodies) != 2 || sender.keys[0] == sender.keys[1] {
		t.Fatalf("resources were mixed or routed with the same key: %+v", sender)
	}
	var first exportWindow
	if err := json.Unmarshal(sender.bodies[0], &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Metrics) != 2 || first.Metrics[1].Labels["resourceID"] != strings.ToUpper(platformResourceID) {
		t.Fatalf("case-insensitive grouping changed original resource labels: %+v", first)
	}
}

func TestPlatformResourceFallbackAndMissingMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		path    string
		targets []string
		wantID  string
	}{
		{"explicit resource", "/probe/metrics/resource", []string{platformResourceID}, platformResourceID},
		{"multiple targets", "/probe/metrics/resource", []string{platformResourceID, platformResourceID + "2"}, ""},
		{"discovery does not use target", "/probe/metrics/list", []string{platformResourceID}, ""},
		{"subscription without resource ID", "/probe/metrics", nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			batch := platformTestBatch()
			batch.MetricsPath = test.path
			batch.Params["target"] = test.targets
			batch.Window.Metrics[0].Labels = map[string]string{"aggregation": "average"}
			var logs bytes.Buffer
			sender := &recordingSender{}
			err := eventHubSink(sender, slog.New(slog.NewTextHandler(&logs, nil)))(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}
			var got exportWindow
			if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
				t.Fatal(err)
			}
			if got.ResourceID != test.wantID || got.Region != "" ||
				!strings.Contains(logs.String(), "exporting empty region") ||
				(test.wantID == "" && !strings.Contains(logs.String(), "exporting without resourceId")) {
				t.Fatalf("incorrect missing metadata policy: %+v, %s", got, &logs)
			}
		})
	}
}

func TestPlatformSplitPreservesSamplesAndMetadata(t *testing.T) {
	for _, manySeries := range []bool{false, true} {
		t.Run(fmt.Sprint(manySeries), func(t *testing.T) {
			batch := platformTestBatch()
			base := batch.Window.Metrics[0]
			start := base.Samples[0].Timestamp
			batch.Window.Metrics = nil
			const count = 12000
			if !manySeries {
				base.Samples = nil
				batch.Window.Metrics = append(batch.Window.Metrics, base)
			}
			for i := range count {
				point := metricdata.Sample{Timestamp: start.Add(time.Duration(i) * time.Minute), Value: float64(i)}
				if manySeries {
					batch.Window.Metrics = append(batch.Window.Metrics, metricdata.Series{
						Name: fmt.Sprintf("metric_%d", i), Labels: base.Labels, Samples: []metricdata.Sample{point},
					})
				} else {
					batch.Window.Metrics[0].Samples = append(batch.Window.Metrics[0].Samples, point)
				}
			}
			sender := &recordingSender{}
			if err := eventHubSink(sender, quietLogger())(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			if len(sender.bodies) < 2 {
				t.Fatal("oversized platform data not split")
			}
			next := 0
			for i, body := range sender.bodies {
				if len(body) > eventBodyLimit || sender.keys[i] != sender.keys[0] {
					t.Fatal("split exceeded event limit or changed resource partition key")
				}
				var window exportWindow
				if err := json.Unmarshal(body, &window); err != nil {
					t.Fatal(err)
				}
				if window.Source != metricdata.Source || window.ResourceID != platformResourceID ||
					window.Query == nil || !reflect.DeepEqual(window.Query.Params, batch.Params) ||
					!window.WindowStart.Equal(start) || !window.WindowEnd.Equal(start.Add((count-1)*time.Minute)) {
					t.Fatal("split changed platform envelope metadata")
				}
				for _, series := range window.Metrics {
					if !reflect.DeepEqual(series.Labels, base.Labels) {
						t.Fatal("split lost dimensions")
					}
					for _, point := range series.Samples {
						if point.Value != float64(next) || !point.Timestamp.Equal(start.Add(time.Duration(next)*time.Minute)) {
							t.Fatalf("split lost or modified sample %d", next)
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

func TestPlatformJobsAndSourceValidation(t *testing.T) {
	paths := []string{"/probe/metrics/resource", "/probe/metrics/list", "/probe/metrics",
		"/probe/metrics/scrape", "/probe/metrics/resourcegraph"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			yaml := fmt.Sprintf(`
event_hub:
  namespace: example.servicebus.windows.net
  name: metrics
scrape_configs:
- job_name: platform
  metrics_path: %s
  params:
    format: [json]
  static_configs:
  - targets: [localhost:8080]
`, path)
			if _, err := ParseConfig([]byte(yaml)); err != nil {
				t.Fatal(err)
			}
			if _, err := ParseConfig([]byte(strings.Replace(yaml, "[json]", "[prometheus]", 1))); err == nil {
				t.Fatal("platform text job accepted for Event Hub")
			}
			for _, source := range []string{metricdata.Source, metricdata.PrometheusSource} {
				batch := platformTestBatch()
				batch.Window.Source = source
				body, err := json.Marshal(batch.Window)
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != path || r.URL.Query().Get("metric") != "Percentage CPU" ||
						len(r.URL.Query()["aggregation"]) != 2 || r.Header.Get("Accept") != "application/json" {
						t.Errorf("platform query changed: %s", r.URL)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(body)
				}))
				defer server.Close()
				target := strings.TrimPrefix(server.URL, "http://")
				job := testJob(target)
				job.MetricsPath, job.Params = path, batch.Params
				sender := &recordingSender{}
				s := New(&Config{EventHub: &EventHubConfig{}}, quietLogger(), eventHubSink(sender, quietLogger()))
				defer s.Client.CloseIdleConnections()
				err = s.Scrape(context.Background(), job, target, nil)
				if source == metricdata.Source {
					if err != nil || len(sender.bodies) != 1 {
						t.Fatalf("platform scrape failed: %v", err)
					}
					var got exportWindow
					if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
						t.Fatal(err)
					}
					if got.Query == nil || got.Query.Path != path || !reflect.DeepEqual(got.Query.Params, batch.Params) {
						t.Fatal("scraper did not wire platform query metadata")
					}
				} else if err == nil || len(sender.bodies) != 0 {
					t.Fatal("mismatched platform response source was sent")
				}
			}
		})
	}
}
