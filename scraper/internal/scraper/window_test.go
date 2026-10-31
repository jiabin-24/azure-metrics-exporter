package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/webdevops/azure-metrics-exporter/config"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
	"github.com/webdevops/azure-metrics-exporter/metrics"
)

func TestScrapePlatformWindowEndToEnd(t *testing.T) {
	store := cache.New(time.Minute, 0)
	list := metrics.NewMetricList()
	start := time.Date(2026, 10, 21, 14, 0, 0, 123000000, time.UTC)
	for _, node := range []string{"A", "B"} {
		for i := range 13 {
			list.Add("cpu", metrics.MetricRow{Labels: prometheus.Labels{
				"node": node, "aggregation": "average", "resourceID": platformResourceID, "region": "eastus",
			},
				Value: float64(i) + 0.5, Timestamp: start.Add(time.Duration(i) * 15 * time.Second)})
		}
	}
	store.Set("window", list, cache.DefaultExpiration)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("incorrect JSON request: %s %s", r.URL, r.Header.Get("Accept"))
		}
		prober := metrics.NewMetricProber(r.Context(), quietLogger(), w,
			&metrics.RequestMetricSettings{Format: "json"}, config.Opts{})
		prober.EnableMetricsCache(store, "window", nil)
		if !prober.FetchFromCache() {
			t.Error("missing test window")
		}
		prober.WriteResponse(w, r)
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	job.MetricsPath = "/probe/metrics/resource"
	job.Params = map[string][]string{"format": {"json"}}
	var got Batch
	sender := &recordingSender{}
	sink := eventHubSink(sender, quietLogger())
	s := New(&Config{EventHub: &EventHubConfig{}}, quietLogger(), func(ctx context.Context, batch Batch) error {
		got = batch
		return sink(ctx, batch)
	})
	defer s.Client.CloseIdleConnections()
	if err := s.Scrape(context.Background(), job, target, map[string]string{"env": "test"}); err != nil {
		t.Fatal(err)
	}
	if got.Window == nil || got.Families != 1 || len(got.Window.Metrics) != 2 || got.Labels["env"] != "test" {
		t.Fatalf("incorrect window batch: %+v", got)
	}
	for _, series := range got.Window.Metrics {
		if len(series.Samples) != 13 {
			t.Fatalf("lost range samples: %+v", series)
		}
		for i, point := range series.Samples {
			if !point.Timestamp.Equal(start.Add(time.Duration(i)*15*time.Second)) || point.Value != float64(i)+0.5 {
				t.Fatalf("sample was modified: %+v", point)
			}
		}
	}
	if !strings.Contains(string(got.Metrics), `["2026-10-21T14:00:00.123Z",0.5]`) {
		t.Fatalf("raw JSON was not preserved: %s", got.Metrics)
	}
	if len(sender.bodies) != 1 {
		t.Fatalf("expected one platform event, got %d", len(sender.bodies))
	}
	var exported exportWindow
	if err := json.Unmarshal(sender.bodies[0], &exported); err != nil {
		t.Fatal(err)
	}
	if exported.Source != "azure-monitor" || exported.ResourceID != platformResourceID ||
		len(exported.Metrics) != 2 || !exported.WindowStart.Equal(start) ||
		!exported.WindowEnd.Equal(start.Add(3*time.Minute)) {
		t.Fatalf("incorrect platform event: %+v", exported)
	}
	for _, series := range exported.Metrics {
		if len(series.Samples) != 13 || series.Labels["aggregation"] != "average" || series.Labels["node"] == "" {
			t.Fatal("Event Hub conversion lost platform dimensions or historical samples")
		}
		for i, point := range series.Samples {
			if !point.Timestamp.Equal(start.Add(time.Duration(i)*15*time.Second)) || point.Value != float64(i)+0.5 {
				t.Fatalf("Event Hub conversion modified source sample: %+v", point)
			}
		}
	}
}

func TestScrapeRejectsInvalidPlatformJSON(t *testing.T) {
	for _, test := range []struct {
		name, contentType, body string
	}{
		{"error object", "application/json", `{"error":"Azure failed"}`},
		{"wrong schema", "application/json", `{"schemaVersion":"2.0","source":"azure-monitor","metrics":[]}`},
		{"invalid sample", "application/json", `{"schemaVersion":"1.0","source":"azure-monitor","metrics":[{"name":"cpu","labels":{},"samples":[["bad",1]]}]}`},
		{"text fallback", "text/plain", "up 1\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			target := strings.TrimPrefix(server.URL, "http://")
			job := testJob(target)
			job.Params = map[string][]string{"format": {"json"}}
			s := New(&Config{}, quietLogger(), func(context.Context, Batch) error {
				t.Error("invalid JSON must not reach sink")
				return nil
			})
			defer s.Client.CloseIdleConnections()
			if err := s.Scrape(context.Background(), job, target, nil); err == nil {
				t.Fatal("invalid JSON was accepted")
			}
		})
	}
}

func TestScrapePromQLWindowUsesSameJSONFormat(t *testing.T) {
	const payload = `{"schemaVersion":"1.0","source":"azure-prometheus","metrics":[{"name":"azure_promql_query_result","labels":{"cluster":"prod","microsoft.resourceid":"/resources/example"},"samples":[["2026-10-21T14:00:00Z",1],["2026-10-21T14:00:15Z",2]]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" || r.Header.Get("Accept") != "application/json" ||
			r.URL.Query().Get("honor_timestamps") != "false" {
			t.Errorf("incorrect PromQL JSON request: %s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, payload)
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	honorTimestamps := false
	job.HonorTimestamps = &honorTimestamps
	job.MetricsPath = "/probe/metrics/promql"
	job.Params = map[string][]string{"query": {"sum(rate(up[5m]))"}, "format": {"json"}}
	var got Batch
	s := New(&Config{}, quietLogger(), func(_ context.Context, batch Batch) error { got = batch; return nil })
	defer s.Client.CloseIdleConnections()
	if err := s.Scrape(context.Background(), job, target, nil); err != nil {
		t.Fatal(err)
	}
	if got.Window == nil || got.Window.Source != "azure-prometheus" || got.Families != 1 ||
		len(got.Window.Metrics[0].Samples) != 2 ||
		got.Window.Metrics[0].Labels["microsoft.resourceid"] != "/resources/example" ||
		string(got.Metrics) != payload {
		t.Fatalf("PromQL window was not preserved: %+v", got)
	}
}

func TestScrapePromQLMetricNameEndToEnd(t *testing.T) {
	const query = "1 - (etcd_mvcc_db_total_size_in_use_in_bytes / etcd_mvcc_db_total_size_in_bytes)"
	for _, originalName := range []string{"azure_promql_query_result", "etcd_mvcc_db_total_size_in_bytes"} {
		t.Run(originalName, func(t *testing.T) {
			start := time.Date(2026, 10, 15, 12, 24, 15, 0, time.UTC)
			window := metricdata.Window{
				SchemaVersion: metricdata.SchemaVersion, Source: metricdata.PrometheusSource,
				Metrics: []metricdata.Series{},
			}
			for _, instance := range []string{"etcd-1", "etcd-2"} {
				window.Metrics = append(window.Metrics, metricdata.Series{
					Name: originalName,
					Labels: map[string]string{
						"cluster": "aks-ae-cluster-1", "region": "australiaeast", "instance": instance,
						"microsoft.resourceid": "/resources/example",
					},
					Samples: []metricdata.Sample{
						{Timestamp: start, Value: 0.5891350210970464},
						{Timestamp: start.Add(15 * time.Second), Value: 0.625},
					},
				})
			}
			payload, err := json.Marshal(window)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				params := r.URL.Query()
				if params.Get("query") != query || params.Has("metric_name") ||
					params.Get("format") != "json" || params.Get("step") != "15s" {
					t.Errorf("metric alias changed the upstream query: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				if _, err := w.Write(payload); err != nil {
					t.Errorf("write window: %v", err)
				}
			}))
			defer server.Close()
			target := strings.TrimPrefix(server.URL, "http://")
			job := testJob(target)
			job.MetricName = "etcd_db_fragmentation_ratio"
			job.Params = map[string][]string{"query": {query}, "format": {"json"}}
			job.RollingRange = &RollingRange{Window: Duration(time.Minute), Step: Duration(15 * time.Second)}
			sender := &recordingSender{}
			sink := eventHubSink(sender, quietLogger())
			var got Batch
			s := New(&Config{EventHub: &EventHubConfig{}}, quietLogger(), func(ctx context.Context, batch Batch) error {
				got = batch
				return sink(ctx, batch)
			})
			defer s.Client.CloseIdleConnections()
			if err := s.Scrape(context.Background(), job, target, nil); err != nil {
				t.Fatal(err)
			}
			for i := range window.Metrics {
				window.Metrics[i].Name = job.MetricName
			}
			rawWindow, err := metricdata.Decode(got.Metrics)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Window, &window) || !reflect.DeepEqual(rawWindow, &window) ||
				got.Families != 1 || got.Query != query {
				t.Fatalf("metric alias did not preserve samples, labels and query across batch representations: %+v", got)
			}
			if len(sender.bodies) != 1 {
				t.Fatalf("expected one Event Hub event, got %d", len(sender.bodies))
			}
			var exported exportWindow
			if err := json.Unmarshal(sender.bodies[0], &exported); err != nil {
				t.Fatal(err)
			}
			if len(exported.Metrics) != len(window.Metrics) {
				t.Fatalf("Event Hub event lost series: %+v", exported)
			}
			for i, series := range exported.Metrics {
				want := window.Metrics[i]
				if series.Name != want.Name || series.PromQL != query ||
					!reflect.DeepEqual(series.Labels, want.Labels) || !reflect.DeepEqual(series.Samples, want.Samples) {
					t.Fatalf("Event Hub metric does not match the configured alias and original data: %+v", series)
				}
			}
		})
	}
}

func TestScrapeMetricNameRejectsPlatformSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"schemaVersion":"1.0","source":"azure-monitor","metrics":[]}`)
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	job.Params = map[string][]string{"query": {"up"}, "format": {"json"}}
	job.MetricName = "custom_metric"
	s := New(&Config{}, quietLogger(), func(context.Context, Batch) error {
		t.Error("invalid source must not reach sink")
		return nil
	})
	defer s.Client.CloseIdleConnections()
	if err := s.Scrape(context.Background(), job, target, nil); err == nil || !strings.Contains(err.Error(), "metric_name") {
		t.Fatalf("expected metric_name source error, got %v", err)
	}
}
