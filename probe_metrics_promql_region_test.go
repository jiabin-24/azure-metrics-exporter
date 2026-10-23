package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
	"github.com/webdevops/azure-metrics-exporter/internal/requestgroup"
)

type promQLResourceGetterFunc func(context.Context, string) (*armresources.GenericResourceExpanded, error)

func (f promQLResourceGetterFunc) GetCachedResource(ctx context.Context, id string) (*armresources.GenericResourceExpanded, error) {
	return f(ctx, id)
}

type promQLLookupWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *promQLLookupWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func waitForPromQLLookup[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resource lookup")
		var zero T
		return zero
	}
}

func TestAzureResourceLookupsCoalesce(t *testing.T) {
	location := "australiaeast"
	tag := "production"
	resource := &armresources.GenericResourceExpanded{Location: &location, Tags: map[string]*string{"environment": &tag}}
	failure := errors.New("resource lookup failed")
	for _, scenario := range []struct {
		name     string
		resource *armresources.GenericResourceExpanded
		err      error
	}{
		{"success", resource, nil},
		{"not found", nil, nil},
		{"failure", nil, failure},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var group azureResourceLookupGroup
			var calls atomic.Int32
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			resources := promQLResourceGetterFunc(func(ctx context.Context, id string) (*armresources.GenericResourceExpanded, error) {
				calls.Add(1)
				if id != "/resources/test" {
					t.Errorf("lookup used a non-normalized resource ID: %q", id)
				}
				select {
				case <-release:
					return scenario.resource, scenario.err
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			type result struct {
				resource *armresources.GenericResourceExpanded
				err      error
			}
			results := make(chan result, 1)
			ctx := &promQLLookupWaitContext{Context: t.Context(), waiting: make(chan struct{})}
			go func() {
				resource, err := group.get(ctx, resources, "/resources/test")
				results <- result{resource, err}
			}()
			waitForPromQLLookup(t, ctx.waiting)
			for i := range 17 {
				id := "/resources/test"
				if i%2 == 0 {
					id = " /RESOURCES/TEST "
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				resource, err := group.get(ctx, resources, id)
				cancel()
				if resource != nil || !errors.Is(err, requestgroup.ErrInFlight) {
					t.Fatalf("in-flight resource lookup must be skipped without waiting: resource=%v err=%v", resource, err)
				}
			}
			unblock()
			got := waitForPromQLLookup(t, results)
			if got.resource != scenario.resource || !errors.Is(got.err, scenario.err) {
				t.Fatalf("initiating caller did not receive the resource result: %+v", got)
			}
			if calls.Load() != 1 {
				t.Fatalf("18 concurrent requests made %d underlying calls, want 1", calls.Load())
			}
			resource, err := group.get(t.Context(), resources, "/resources/test")
			if resource != scenario.resource || !errors.Is(err, scenario.err) || calls.Load() != 2 {
				t.Fatalf("completed lookups must leave caching/retry to the underlying getter: calls=%d resource=%v err=%v",
					calls.Load(), resource, err)
			}
		})
	}
}

func TestAzureResourceLookupsDifferentResourcesRunConcurrently(t *testing.T) {
	var group azureResourceLookupGroup
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	resources := promQLResourceGetterFunc(func(ctx context.Context, id string) (*armresources.GenericResourceExpanded, error) {
		started <- id
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	results := make(chan error, 2)
	for _, id := range []string{"/resources/first", "/resources/second"} {
		go func() {
			_, err := group.get(t.Context(), resources, id)
			results <- err
		}()
	}
	first := waitForPromQLLookup(t, started)
	second := waitForPromQLLookup(t, started)
	if first == second {
		t.Fatal("different resources were incorrectly merged")
	}
	unblock()
	for range 2 {
		if err := waitForPromQLLookup(t, results); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAzureResourceLookupCancellationKeepsSharedRequestRunning(t *testing.T) {
	var group azureResourceLookupGroup
	var calls atomic.Int32
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	location := "australiaeast"
	resource := &armresources.GenericResourceExpanded{Location: &location}
	resources := promQLResourceGetterFunc(func(ctx context.Context, _ string) (*armresources.GenericResourceExpanded, error) {
		calls.Add(1)
		started <- ctx
		select {
		case <-release:
			return resource, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := group.get(ctx, resources, "/resources/test")
		first <- err
	}()
	lookupCtx := waitForPromQLLookup(t, started)
	if deadline, ok := lookupCtx.Deadline(); !ok || time.Until(deadline) > azureResourceLookupTimeout {
		t.Fatal("shared resource lookup must retain its own bounded deadline")
	}
	waiterCtx := &promQLLookupWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	second := make(chan error, 1)
	go func() {
		got, err := group.requests.Do(waiterCtx, "/resources/test", azureResourceLookupTimeout,
			func(context.Context) (*armresources.GenericResourceExpanded, error) {
				return nil, errors.New("completion observer started a duplicate query")
			})
		if err == nil && got != resource {
			err = errors.New("other waiter lost the shared resource result")
		}
		second <- err
	}()
	waitForPromQLLookup(t, waiterCtx.waiting)
	cancel()
	if err := waitForPromQLLookup(t, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter did not stop waiting: %v", err)
	}
	if err := lookupCtx.Err(); err != nil {
		t.Fatalf("one waiter cancelled the shared lookup: %v", err)
	}
	unblock()
	if err := waitForPromQLLookup(t, second); err != nil || calls.Load() != 1 {
		t.Fatalf("other waiter should succeed with one underlying call: calls=%d err=%v", calls.Load(), err)
	}
}

func TestAzureResourceLookupAlreadyCancelledDoesNotStart(t *testing.T) {
	var group azureResourceLookupGroup
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	resources := promQLResourceGetterFunc(func(context.Context, string) (*armresources.GenericResourceExpanded, error) {
		t.Error("already cancelled request started an underlying lookup")
		return nil, nil
	})
	if _, err := group.get(ctx, resources, "/resources/test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestPromQLRegionEnrichmentDeduplicatesResourcesAndPreservesLabels(t *testing.T) {
	east, west := "eastasia", "westus"
	first := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerService/managedClusters/aks"
	second := first + "-other"
	samples := []promQLSample{
		{name: "requests", labels: map[string]string{"microsoft.resourceid": first, "cluster": "prod"}, value: 12, timestampMillis: 123},
		{name: "requests", labels: map[string]string{"microsoft.resourceid": strings.ToUpper(first), "code": "200"}, value: 13, timestampMillis: 456},
		{name: "requests", labels: map[string]string{"resourceID": second}},
		{name: "requests", labels: map[string]string{"microsoft_resourceid": first}},
		{name: "requests", labels: map[string]string{"microsoft.resourceid": first, "region": "keep-region"}},
		{name: "requests", labels: map[string]string{"microsoft.resourceid": first, "location": "keep-location"}},
		{name: "sum", labels: map[string]string{"cluster": "prod"}},
	}
	calls := make(map[string]int)
	enricher := promQLRegionEnricher{
		resources: promQLResourceGetterFunc(func(_ context.Context, id string) (*armresources.GenericResourceExpanded, error) {
			calls[id]++
			location := &east
			if id == strings.ToLower(second) {
				location = &west
			}
			return &armresources.GenericResourceExpanded{Location: location}, nil
		}),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	enricher.enrich(context.Background(), samples)
	if len(calls) != 2 || calls[strings.ToLower(first)] != 1 || calls[strings.ToLower(second)] != 1 {
		t.Fatalf("resources not deduplicated case-insensitively: %v", calls)
	}
	for _, i := range []int{0, 1, 3} {
		if samples[i].labels["region"] != east {
			t.Fatalf("missing region on sample %d", i)
		}
	}
	if samples[2].labels["region"] != west || samples[4].labels["region"] != "keep-region" ||
		samples[5].labels["location"] != "keep-location" || samples[5].labels["region"] != "" ||
		samples[6].labels["region"] != "" || samples[0].labels["microsoft.resourceid"] != first ||
		samples[0].value != 12 || samples[0].timestampMillis != 123 ||
		samples[1].value != 13 || samples[1].timestampMillis != 456 {
		t.Fatalf("existing metadata or source samples changed: %+v", samples)
	}
}

func TestPromQLRegionEnrichmentFailuresPreserveMetricsAndWarn(t *testing.T) {
	for _, scenario := range []string{"not found", "missing location", "empty location", "forbidden", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			var logs bytes.Buffer
			calls := 0
			enricher := promQLRegionEnricher{
				resources: promQLResourceGetterFunc(func(ctx context.Context, _ string) (*armresources.GenericResourceExpanded, error) {
					calls++
					switch scenario {
					case "not found":
						return nil, nil
					case "missing location":
						return &armresources.GenericResourceExpanded{}, nil
					case "empty location":
						empty := " "
						return &armresources.GenericResourceExpanded{Location: &empty}, nil
					default:
						return nil, errors.New("forbidden")
					}
				}),
				logger: slog.New(slog.NewTextHandler(&logs, nil)),
			}
			samples := []promQLSample{{name: "requests", labels: map[string]string{"microsoft.resourceid": "/resources/test"}, value: 7, timestampMillis: 123}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "cancelled" {
				cancel()
			}
			enricher.enrich(ctx, samples)
			if samples[0].labels["region"] != "" || len(samples[0].labels) != 1 ||
				samples[0].value != 7 || samples[0].timestampMillis != 123 || !strings.Contains(logs.String(), "WARN") {
				t.Fatalf("lookup failure was silent or changed metrics: %+v, %s", samples, &logs)
			}
			if scenario == "cancelled" && calls != 0 {
				t.Fatal("cancelled lookup still called Azure")
			}
		})
	}
}

func TestPromQLRegionLookupUsesRequestDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	done := make(chan struct{})
	defer func() {
		close(release)
		<-done
	}()
	var logs bytes.Buffer
	enricher := promQLRegionEnricher{
		resources: promQLResourceGetterFunc(func(ctx context.Context, _ string) (*armresources.GenericResourceExpanded, error) {
			defer close(done)
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > azureResourceLookupTimeout {
				t.Error("metadata lookup has no bounded deadline")
			}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}),
		logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	start := time.Now()
	enricher.enrich(ctx, []promQLSample{{labels: map[string]string{"resourceID": "/resources/test"}}})
	if time.Since(start) > time.Second || !strings.Contains(logs.String(), "deadline exceeded") {
		t.Fatalf("lookup did not honor the original request deadline: %s", &logs)
	}
}

func TestPromQLRegionAddedAfterSourceTimestampMatching(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := promQLWindowFixture(t, "requests_total", strings.Contains(r.URL.Query().Get("query"), "timestamp("))
		_, _ = w.Write(body)
	}))
	defer server.Close()
	calls := 0
	enricher := &promQLRegionEnricher{
		resources: promQLResourceGetterFunc(func(_ context.Context, id string) (*armresources.GenericResourceExpanded, error) {
			calls++
			if id != "/resources/example" {
				t.Errorf("wrong lookup id: %s", id)
			}
			location := "eastasia"
			return &armresources.GenericResourceExpanded{Location: &location}, nil
		}),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	params := url.Values{
		"query": {"requests_total"}, "format": {"json"}, "start": {"2026-10-21T14:00:00Z"},
		"end": {"2026-10-21T14:03:00Z"}, "step": {"15s"},
	}
	body, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client(), enricher)
	if err != nil || status != http.StatusOK {
		t.Fatalf("enriched JSON query failed: %d, %v", status, err)
	}
	window, err := metricdata.Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(window.Metrics) != 2 {
		t.Fatalf("duplicate lookups or lost series: calls=%d series=%d", calls, len(window.Metrics))
	}
	for _, series := range window.Metrics {
		if series.Labels["region"] != "eastasia" || series.Labels["cluster"] != "prod" || len(series.Samples) != 13 {
			t.Fatalf("enrichment lost labels or samples: %+v", series)
		}
		for i, point := range series.Samples {
			expected := time.Date(2026, 10, 21, 13, 59, 53, 0, time.UTC).Add(time.Duration(i) * 15 * time.Second)
			if !point.Timestamp.Equal(expected) || point.Value != float64(i)+0.5 {
				t.Fatalf("enrichment changed source timestamp or value: %+v", point)
			}
		}
	}
	params.Set("format", "prometheus")
	text, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client(), enricher)
	if err != nil || status != http.StatusOK || strings.Count(text, `region="eastasia"`) != 2 || calls != 2 {
		t.Fatalf("text response missing enrichment: calls=%d status=%d err=%v text=%s", calls, status, err, text)
	}
	baseline, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client(), nil)
	if err != nil || status != http.StatusOK || strings.ReplaceAll(text, `,region="eastasia"`, "") != baseline {
		t.Fatal("text enrichment changed more than the region label")
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "requests_total{") &&
			(!strings.Contains(line, `microsoft_resourceid="/resources/example"`) ||
				!strings.HasSuffix(line, " 12.5 1792591373000")) {
			t.Fatalf("text enrichment changed the latest source sample or label normalization: %s", line)
		}
	}
}

func TestPromQLInstantRegionEnrichmentAcrossFormats(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("instant query used wrong path: %s", r.URL.Path)
		}
		if strings.Contains(r.URL.Query().Get("query"), "timestamp(") {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"cluster":"prod","microsoft.resourceid":"/resources/example","azure_metrics_exporter_source_name":"requests_total"},"value":[1792591200.125,"1792591193.125"]}]}}`)
		} else {
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"requests_total","cluster":"prod","microsoft.resourceid":"/resources/example"},"value":[1792591200.125,"1.25"]}]}}`)
		}
	}))
	defer server.Close()
	for _, format := range []string{"json", "prometheus", ""} {
		t.Run("format="+format, func(t *testing.T) {
			calls := 0
			enricher := &promQLRegionEnricher{
				resources: promQLResourceGetterFunc(func(_ context.Context, id string) (*armresources.GenericResourceExpanded, error) {
					calls++
					if id != "/resources/example" {
						t.Errorf("wrong lookup id: %s", id)
					}
					location := "eastasia"
					return &armresources.GenericResourceExpanded{Location: &location}, nil
				}),
				logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			params := url.Values{"query": {"requests_total"}}
			if format != "" {
				params.Set("format", format)
			}
			body, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
				promQLWindowCredential{}, server.Client(), enricher)
			if err != nil || status != http.StatusOK || calls != 1 {
				t.Fatalf("instant enrichment failed: calls=%d status=%d err=%v", calls, status, err)
			}
			if format == "json" {
				window, err := metricdata.Decode([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				if len(window.Metrics) != 1 {
					t.Fatalf("lost instant series: %+v", window)
				}
				series := window.Metrics[0]
				if series.Name != "requests_total" || series.Labels["region"] != "eastasia" ||
					series.Labels["microsoft.resourceid"] != "/resources/example" || len(series.Samples) != 1 ||
					series.Samples[0].Timestamp.UnixMilli() != 1792591193125 || series.Samples[0].Value != 1.25 {
					t.Fatalf("instant JSON metadata or source sample changed: %+v", series)
				}
			} else if !strings.Contains(body, "requests_total{cluster=\"prod\",microsoft_resourceid=\"/resources/example\",region=\"eastasia\"} 1.25 1792591193125\n") {
				t.Fatalf("instant text metadata or source sample changed: %s", body)
			}
		})
	}
}
