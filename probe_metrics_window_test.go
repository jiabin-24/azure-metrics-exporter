package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/webdevops/go-common/log/slogger"

	"github.com/webdevops/azure-metrics-exporter/config"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
	"github.com/webdevops/azure-metrics-exporter/metrics"
)

func TestPlatformHandlersWindowAndDefaultCompatibility(t *testing.T) {
	oldOpts, oldLogger, oldCache, oldRequests := Opts, logger, metricsCache, prometheusMetricRequests
	t.Cleanup(func() {
		Opts, logger, metricsCache, prometheusMetricRequests = oldOpts, oldLogger, oldCache, oldRequests
	})
	Opts = config.Opts{}
	Opts.Prober.Cache = true
	duration := time.Duration(0)
	Opts.Azure.ServiceDiscovery.CacheDuration = &duration
	logger = slogger.NewCliLogger(io.Discard)
	metricsCache = cache.New(time.Minute, 0)
	prometheusMetricRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_platform_requests"},
		[]string{"subscriptionID", "handler", "filter", "result"})
	start := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	list := metrics.NewMetricList()
	for i := range 3 {
		list.Add("cpu", metrics.MetricRow{
			Labels: prometheus.Labels{"aggregation": "average"}, Value: float64(i + 1),
			Timestamp: start.Add(time.Duration(i) * time.Minute),
		})
	}
	for _, handler := range []struct {
		path, prefix string
		fn           http.HandlerFunc
	}{
		{config.ProbeMetricsResourceUrl, "resource", probeMetricsResourceHandler},
		{config.ProbeMetricsListUrl, "list", probeMetricsListHandler},
		{config.ProbeMetricsSubscriptionUrl, "list", probeMetricsSubscriptionHandler},
		{config.ProbeMetricsScrapeUrl, "scrape", probeMetricsScrapeHandler},
		{config.ProbeMetricsResourceGraphUrl, "scrape", probeMetricsResourceGraphHandler},
	} {
		for _, format := range []string{"", "json", "invalid"} {
			t.Run(handler.path+"/"+format, func(t *testing.T) {
				params := url.Values{
					"subscription": {"00000000-0000-0000-0000-000000000001"},
					"target":       {"/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/test/providers/Microsoft.ContainerService/managedClusters/test"},
					"resourceType": {"Microsoft.ContainerService/managedClusters"},
					"metric":       {"node_cpu_usage_percentage"}, "cache": {"1m"},
					"metricTagName": {"metrics"}, "aggregationTagName": {"aggregations"},
				}
				if format != "" {
					params.Set("format", format)
				}
				req := httptest.NewRequest(http.MethodGet, handler.path+"?"+params.Encode(), nil)
				key := fmt.Sprintf("%s:%x", handler.prefix, sha256.Sum256([]byte(req.URL.String())))
				metricsCache.Set(key, list, cache.DefaultExpiration)
				w := httptest.NewRecorder()
				handler.fn(w, req)
				if format == "invalid" {
					if w.Code != http.StatusBadRequest {
						t.Fatalf("invalid format returned %d", w.Code)
					}
					return
				}
				if w.Code != http.StatusOK || w.Header().Get("X-metrics-cached") != "true" {
					t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
				}
				if format == "json" {
					window, err := metricdata.Decode(w.Body.Bytes())
					if err != nil || len(window.Metrics) != 1 || len(window.Metrics[0].Samples) != 3 {
						t.Fatalf("handler lost history: %v %s", err, w.Body.String())
					}
				} else if !strings.Contains(w.Body.String(), `cpu{aggregation="average"} 3 `) {
					t.Fatalf("default text behavior changed: %s", w.Body.String())
				}
			})
		}
	}
}
