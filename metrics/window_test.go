package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/webdevops/go-common/utils/to"

	"github.com/webdevops/azure-metrics-exporter/config"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

func testProber(format string) *MetricProber {
	p := NewMetricProber(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		httptest.NewRecorder(), &RequestMetricSettings{Format: format}, config.Opts{})
	p.SetPrometheusRegistry(prometheus.NewRegistry())
	return p
}

func TestWindowPreservesAllSeriesPointsAndDefaultText(t *testing.T) {
	p := testProber("json")
	start := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	for _, node := range []string{"B", "A"} {
		for _, second := range []int{120, 0, 60} {
			p.metricList.Add("cpu", MetricRow{
				Labels: prometheus.Labels{"node": node, "aggregation": "average"},
				Value:  float64(second), Timestamp: start.Add(time.Duration(second) * time.Second),
			})
		}
	}
	p.publishMetricList()
	w := httptest.NewRecorder()
	if status := p.WriteResponse(w, httptest.NewRequest(http.MethodGet, "/", nil)); status != http.StatusOK {
		t.Fatalf("unexpected status: %d", status)
	}
	window, err := metricdata.Decode(w.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(window.Metrics) != 2 || window.Metrics[0].Labels["node"] != "A" {
		t.Fatalf("unexpected series ordering: %+v", window)
	}
	for _, series := range window.Metrics {
		if len(series.Samples) != 3 {
			t.Fatalf("history was lost: %+v", series)
		}
		for i, sample := range series.Samples {
			if !sample.Timestamp.Equal(start.Add(time.Duration(i)*time.Minute)) || sample.Value != float64(i*60) {
				t.Fatalf("history changed: %+v", series)
			}
		}
	}
	second := httptest.NewRecorder()
	p.WriteResponse(second, httptest.NewRequest(http.MethodGet, "/", nil))
	if second.Body.String() != w.Body.String() {
		t.Fatal("JSON serialization must be deterministic")
	}
	if p.metricList.GetMetricList("cpu")[0].Value != 120 {
		t.Fatal("JSON serialization mutated the original rows")
	}
	p.settings.Format = "prometheus"
	p.publishMetricList()
	text := httptest.NewRecorder()
	p.WriteResponse(text, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(text.Body.String(), `cpu{aggregation="average",node="A"} 60 `) ||
		strings.Contains(text.Body.String(), "samples") {
		t.Fatalf("default output behavior changed: %s", text.Body.String())
	}
}

func TestWindowCachePreservesHistory(t *testing.T) {
	cacheStore := cache.New(time.Minute, 0)
	p := testProber("json")
	for i := range 13 {
		p.metricList.Add("cpu", MetricRow{Labels: prometheus.Labels{"node": "A"}, Value: float64(i),
			Timestamp: time.Unix(int64(i*15+1), 0)})
	}
	duration := time.Minute
	p.EnableMetricsCache(cacheStore, "json-window", &duration)
	p.SaveToCache()
	cached := testProber("json")
	cached.EnableMetricsCache(cacheStore, "json-window", &duration)
	if !cached.FetchFromCache() {
		t.Fatal("expected cache hit")
	}
	w := httptest.NewRecorder()
	cached.WriteResponse(w, httptest.NewRequest(http.MethodGet, "/", nil))
	window, err := metricdata.Decode(w.Body.Bytes())
	if err != nil || len(window.Metrics[0].Samples) != 13 {
		t.Fatalf("cache lost the window: %v %s", err, w.Body.String())
	}
}

func TestIncompleteWindowsFailWithoutCaching(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"upstream", errors.New("Azure unavailable"), http.StatusBadGateway},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := testProber("json")
			p.recordCollectionError(test.err)
			store := cache.New(time.Minute, 0)
			duration := time.Minute
			p.EnableMetricsCache(store, "failed", &duration)
			p.SaveToCache()
			if _, ok := store.Get("failed"); ok {
				t.Fatal("failed JSON collection was cached")
			}
			w := httptest.NewRecorder()
			if status := p.WriteResponse(w, httptest.NewRequest(http.MethodGet, "/", nil)); status != test.status {
				t.Fatalf("unexpected status: %d", status)
			}
			if !strings.Contains(w.Body.String(), test.err.Error()) {
				t.Fatalf("failure was hidden: %s", w.Body.String())
			}
		})
	}
}

func TestInvalidWindowValuesDoNotProduceSuccess(t *testing.T) {
	for _, row := range []MetricRow{
		{Value: 1},
		{Value: math.NaN(), Timestamp: time.Now()},
	} {
		p := testProber("json")
		p.metricList.Add("cpu", row)
		w := httptest.NewRecorder()
		if status := p.WriteResponse(w, httptest.NewRequest(http.MethodGet, "/", nil)); status != http.StatusBadGateway {
			t.Fatalf("invalid sample produced success: %d %s", status, w.Body.String())
		}
	}
}

func TestAzurePointsPreserveAggregationsAndHistory(t *testing.T) {
	p := testProber("json")
	p.settings.Name = "cpu"
	result := AzureInsightBaseMetricsResult{prober: p}
	first, second := time.Unix(100, 0), time.Unix(160, 0)
	points := []*armmonitor.MetricValue{
		{TimeStamp: &first, Average: to.Float64Ptr(1.5), Total: to.Float64Ptr(3)},
		{TimeStamp: &second, Average: to.Float64Ptr(2.5), Total: to.Float64Ptr(5)},
	}
	ch := make(chan PrometheusMetricResult, 4)
	result.sendDataPoints(ch, prometheus.Labels{"resourceID": "/resources/example"}, points)
	close(ch)
	for row := range ch {
		p.metricList.Add(row.Name, MetricRow{Labels: row.Labels, Value: row.Value, Timestamp: row.Timestamp})
	}
	window := p.metricList.window()
	if len(window.Metrics) != 2 {
		t.Fatalf("aggregations were mixed: %+v", window)
	}
	for _, series := range window.Metrics {
		if len(series.Samples) != 2 || !series.Samples[0].Timestamp.Equal(first) ||
			!series.Samples[1].Timestamp.Equal(second) {
			t.Fatalf("Azure timestamps were not preserved: %+v", series)
		}
	}
}

func TestAzurePointErrorsAreReported(t *testing.T) {
	now := time.Now()
	for _, data := range [][]*armmonitor.MetricValue{
		{{Average: to.Float64Ptr(1)}},
		{{TimeStamp: &now, Average: to.Float64Ptr(math.Inf(1))}},
	} {
		p := testProber("json")
		result := AzureInsightBaseMetricsResult{prober: p}
		result.sendDataPoints(make(chan PrometheusMetricResult, 1), prometheus.Labels{}, data)
		if p.collectionError() == nil {
			t.Fatal("invalid source data was not reported")
		}
	}
	p := testProber("json")
	result := AzureInsightBaseMetricsResult{prober: p}
	if !result.metricFailed(to.StringPtr("InvalidMetric"), to.StringPtr("not available")) ||
		p.collectionError() == nil {
		t.Fatal("per-metric Azure error was hidden")
	}
}

func TestFormatSettings(t *testing.T) {
	for _, test := range []struct {
		query string
		want  string
		valid bool
	}{
		{"", "prometheus", true},
		{"&format=prometheus", "prometheus", true},
		{"&format=json", "json", true},
		{"&format=", "", false},
		{"&format=xml", "", false},
		{"&format=json&format=json", "", false},
	} {
		t.Run(test.query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/probe/metrics/resource?subscription=test"+test.query, nil)
			settings, err := NewRequestMetricSettings(req, config.Opts{})
			if (err == nil) != test.valid || (test.valid && settings.Format != test.want) {
				t.Fatalf("unexpected settings: %+v, %v", settings, err)
			}
		})
	}
}
