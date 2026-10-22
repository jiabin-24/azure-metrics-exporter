package scraper

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

const rollingConfig = `
scrape_configs:
- job_name: rolling
  metrics_path: /probe/metrics/promql
  rolling_range: {}
  params:
    query: ['sum(rate(requests_total{cluster="prod"}[5m]))']
    format: [json]
  static_configs:
  - targets: [localhost:8080]
`

func TestRollingRangeDefaultsAndExactBounds(t *testing.T) {
	cfg, err := ParseConfig([]byte(rollingConfig))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	if job.RollingRange.Window != Duration(time.Minute) || job.RollingRange.Step != Duration(15*time.Second) ||
		job.RollingRange.Delay != 0 {
		t.Fatalf("incorrect rolling defaults: %+v", job.RollingRange)
	}
	now := time.Date(2026, 10, 22, 0, 0, 24, 123456789, time.FixedZone("UTC+8", 8*60*60))
	params := job.paramsAt(now)
	if params["start"][0] != "2026-10-21T15:59:24.123Z" ||
		params["end"][0] != "2026-10-21T16:00:24.123Z" || params["step"][0] != "15s" ||
		params["query"][0] != job.Params["query"][0] || params["format"][0] != "json" {
		t.Fatalf("incorrect dynamic range or other parameters: %v", params)
	}
	next := job.paramsAt(now.Add(time.Minute))
	if next["start"][0] != params["end"][0] || next["end"][0] != "2026-10-21T16:01:24.123Z" {
		t.Fatalf("range did not move by one minute: %v -> %v", params, next)
	}
	if _, exists := job.Params["start"]; exists || len(job.Params) != 2 ||
		params["start"][0] != "2026-10-21T15:59:24.123Z" {
		t.Fatal("evaluating another scrape mutated config or a previous batch")
	}
}

func TestRollingRangeCustomWindowDelayAndStep(t *testing.T) {
	cfg, err := ParseConfig([]byte(strings.Replace(rollingConfig, "rolling_range: {}",
		"rolling_range:\n    window: 3m\n    step: 30s\n    delay: 20m", 1)))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	now := time.Date(2026, 10, 22, 0, 10, 24, 0, time.UTC)
	params := job.paramsAt(now)
	if params["start"][0] != "2026-10-21T23:47:24Z" ||
		params["end"][0] != "2026-10-21T23:50:24Z" || params["step"][0] != "30s" {
		t.Fatalf("delay or custom range was not applied: %v", params)
	}
	copy := job
	copy.Params = params
	u, err := url.Parse(copy.targetURL("localhost:8080"))
	if err != nil || !reflect.DeepEqual(map[string][]string(u.Query()), params) {
		t.Fatalf("dynamic parameters did not survive URL encoding: %v, %v", u, err)
	}
}

func TestRollingRangeRejectsInvalidConfiguration(t *testing.T) {
	tests := map[string]string{
		"zero window":          "{window: 0s}",
		"negative window":      "{window: -1m}",
		"zero step":            "{step: 0s}",
		"negative delay":       "{delay: -1s}",
		"invalid duration":     "{window: 1d}",
		"invalid delay":        "{delay: nonsense}",
		"empty delay":          "{delay: ''}",
		"step exceeds window":  "{window: 5s, step: 15s}",
		"submillisecond step":  "{step: 1us}",
		"fractional ms window": "{window: 1.0005s}",
		"fractional ms delay":  "{delay: 500us}",
		"unknown field":        "{alignment: minute}",
	}
	for name, rolling := range tests {
		t.Run(name, func(t *testing.T) {
			input := strings.Replace(rollingConfig, "rolling_range: {}", "rolling_range: "+rolling, 1)
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatal("invalid rolling range was accepted")
			}
		})
	}
	for _, key := range []string{"start", "end", "step", "time", "query_delay"} {
		t.Run("conflicting "+key, func(t *testing.T) {
			for _, value := range []string{"[]", "['']"} {
				input := strings.Replace(rollingConfig, "format: [json]", "format: [json]\n    "+key+": "+value, 1)
				if _, err := ParseConfig([]byte(input)); err == nil || !strings.Contains(err.Error(), "params."+key) {
					t.Fatalf("conflicting parameter was accepted: %v", err)
				}
			}
		})
	}
	for _, path := range []string{"/metrics", "/probe/metrics/resource", "/probe/metrics/list", "/probe/metrics"} {
		input := strings.Replace(rollingConfig, "/probe/metrics/promql", path, 1)
		if _, err := ParseConfig([]byte(input)); err == nil {
			t.Fatalf("rolling range accepted on unsupported endpoint %q", path)
		}
	}
	for _, query := range []string{"[]", "['']", "[up, other]"} {
		input := strings.Replace(rollingConfig, `['sum(rate(requests_total{cluster="prod"}[5m]))']`, query, 1)
		if _, err := ParseConfig([]byte(input)); err == nil {
			t.Fatalf("invalid rolling query accepted: %s", query)
		}
	}
}

func TestNonRollingQueriesRemainUnchanged(t *testing.T) {
	now := time.Now()
	for _, params := range []map[string][]string{
		{"query": {"up"}, "query_delay": {"20m"}},
		{"query": {"up"}, "start": {"2026-10-21T14:00:00Z"}, "end": {"2026-10-21T14:03:00Z"}, "step": {"15s"}},
		{"format": {"json"}, "timespan": {"PT3M"}, "interval": {"PT1M"}},
	} {
		job := Job{Params: params}
		first, second := job.paramsAt(now), job.paramsAt(now.Add(time.Minute))
		if !reflect.DeepEqual(first, params) || !reflect.DeepEqual(second, params) {
			t.Fatalf("existing static query behavior changed: %v", params)
		}
	}
}

func TestRollingScheduledScrapesReachEventHubAfterFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		start, startErr := time.Parse(time.RFC3339Nano, query.Get("start"))
		end, endErr := time.Parse(time.RFC3339Nano, query.Get("end"))
		if startErr != nil || endErr != nil || end.Sub(start) != time.Minute ||
			query.Get("step") != "15s" || query.Has("query_delay") || query.Has("rolling_range") {
			t.Errorf("incorrect scheduled request: %s", r.URL)
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		points := make([]metricdata.Sample, 0, 5)
		for i := range 5 {
			points = append(points, metricdata.Sample{Timestamp: start.Add(time.Duration(i) * 15 * time.Second), Value: float64(i)})
		}
		window := metricdata.Window{
			SchemaVersion: "1.0", Source: metricdata.PrometheusSource,
			Metrics: []metricdata.Series{{
				Name: "requests_total", Labels: map[string]string{"cluster": "prod", "region": "eastus"}, Samples: points,
			}},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(window); err != nil {
			t.Errorf("write range response: %v", err)
		}
	}))
	defer server.Close()
	input := strings.Replace(rollingConfig, "localhost:8080", strings.TrimPrefix(server.URL, "http://"), 1)
	input = strings.Replace(input, "job_name: rolling", "job_name: rolling\n  scrape_interval: 100ms", 1)
	cfg, err := ParseConfig([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	sender := &recordingSender{}
	sink := eventHubSink(sender, quietLogger())
	batches := make(chan Batch, 5)
	s := New(cfg, quietLogger(), func(ctx context.Context, batch Batch) error {
		if err := sink(ctx, batch); err != nil {
			return err
		}
		batches <- batch
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	var got []Batch
	for range 2 {
		select {
		case batch := <-batches:
			got = append(got, batch)
		case <-ctx.Done():
			t.Fatal("scheduled range query did not recover from the first failed scrape")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rolling scraper did not shut down")
	}
	firstEnd, err := time.Parse(time.RFC3339Nano, got[0].Params["end"][0])
	if err != nil {
		t.Fatal(err)
	}
	secondEnd, err := time.Parse(time.RFC3339Nano, got[1].Params["end"][0])
	if err != nil {
		t.Fatal(err)
	}
	if !secondEnd.After(firstEnd) {
		t.Fatalf("range was reused across scheduled scrapes: %v / %v", got[0].Params, got[1].Params)
	}
	if _, exists := cfg.Jobs[0].Params["start"]; exists {
		t.Fatal("scheduled queries mutated shared configuration")
	}
	for i, batch := range got {
		var exported exportWindow
		if err := json.Unmarshal(sender.bodies[i], &exported); err != nil {
			t.Fatal(err)
		}
		start, err := time.Parse(time.RFC3339Nano, batch.Params["start"][0])
		if err != nil {
			t.Fatal(err)
		}
		if len(exported.Metrics) != 1 || len(exported.Metrics[0].Samples) != 5 ||
			exported.Metrics[0].PromQL != cfg.Jobs[0].Params["query"][0] ||
			!exported.WindowStart.Equal(start) || !exported.WindowEnd.Equal(start.Add(time.Minute)) {
			t.Fatalf("Event Hub message lost dynamic range samples: %+v", exported)
		}
		for j, sample := range exported.Metrics[0].Samples {
			if sample.Value != float64(j) || !sample.Timestamp.Equal(start.Add(time.Duration(j)*15*time.Second)) {
				t.Fatalf("source range sample %d was changed", j)
			}
		}
	}
}
