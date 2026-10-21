package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/webdevops/go-common/log/slogger"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

type promQLWindowCredential struct{}

func (promQLWindowCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func promQLWindowFixture(t *testing.T, name string, sourceTimestamps bool) []byte {
	t.Helper()
	start := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	result := make([]azurePrometheusVectorSample, 0, 2)
	for _, instance := range []string{"A", "B"} {
		labels := map[string]string{"instance": instance, "cluster": "prod", "microsoft.resourceid": "/resources/example"}
		if name != "" {
			labels["__name__"] = name
		}
		if sourceTimestamps {
			delete(labels, "__name__")
			labels["azure_metrics_exporter_source_name"] = name
		}
		series := azurePrometheusVectorSample{Metric: labels}
		for i := range 13 {
			evaluation := start.Add(time.Duration(i) * 15 * time.Second).Unix()
			value := strconv.FormatFloat(float64(i)+0.5, 'g', -1, 64)
			if sourceTimestamps {
				value = strconv.FormatInt(evaluation-7, 10)
			}
			encodedValue, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			series.Values = append(series.Values, []json.RawMessage{
				json.RawMessage(strconv.FormatInt(evaluation, 10)), encodedValue,
			})
		}
		result = append(result, series)
	}
	body, err := json.Marshal(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "matrix", "result": result},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestPromQLJSONPreservesEveryPointAndSourceTimestamp(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/query_range" || r.URL.Query().Has("format") ||
			r.URL.Query().Get("step") != "15s" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("incorrect Azure query: %s", r.URL)
		}
		sourceQuery := strings.Contains(r.URL.Query().Get("query"), "timestamp(")
		w.Header().Set("Content-Type", "application/json")
		w.Write(promQLWindowFixture(t, "requests_total", sourceQuery))
	}))
	defer server.Close()
	params := url.Values{
		"query": {"requests_total"}, "format": {"json"},
		"start": {"2026-10-21T14:00:00Z"}, "end": {"2026-10-21T14:03:00Z"}, "step": {"15s"},
	}
	body, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client())
	if err != nil || status != http.StatusOK {
		t.Fatalf("query failed: status=%d error=%v", status, err)
	}
	window, err := metricdata.Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if window.Source != metricdata.PrometheusSource || len(window.Metrics) != 2 || calls.Load() != 2 {
		t.Fatalf("incorrect window: calls=%d %+v", calls.Load(), window)
	}
	start := time.Date(2026, 10, 21, 13, 59, 53, 0, time.UTC)
	for _, series := range window.Metrics {
		if series.Name != "requests_total" || series.Labels["microsoft.resourceid"] != "/resources/example" ||
			len(series.Samples) != 13 {
			t.Fatalf("series lost labels or history: %+v", series)
		}
		for i, sample := range series.Samples {
			if !sample.Timestamp.Equal(start.Add(time.Duration(i)*15*time.Second)) || sample.Value != float64(i)+0.5 {
				t.Fatalf("source sample changed: %+v", sample)
			}
		}
	}
	params.Set("format", "prometheus")
	text, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client())
	if err != nil || status != http.StatusOK {
		t.Fatalf("text query failed: status=%d error=%v", status, err)
	}
	count := 0
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "requests_total{") {
			count++
			if !strings.Contains(line, "microsoft_resourceid=") ||
				!strings.HasSuffix(line, " 12.5 "+strconv.FormatInt(start.Add(3*time.Minute).UnixMilli(), 10)) {
				t.Fatalf("default text output changed: %s", line)
			}
		}
	}
	if count != 2 {
		t.Fatalf("default text output must retain only the last point per series, got %d", count)
	}
}

func TestPromQLJSONExpressionUsesEvaluationTime(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Write(promQLWindowFixture(t, "", false))
	}))
	defer server.Close()
	params := url.Values{
		"query": {"sum(rate(requests_total[5m])) by (instance)"}, "format": {"json"},
		"start": {"2026-10-21T14:00:00Z"}, "end": {"2026-10-21T14:03:00Z"}, "step": {"15s"},
	}
	body, status, err := collectPromQLMetrics(context.Background(), server.URL, params,
		promQLWindowCredential{}, server.Client())
	if err != nil || status != http.StatusOK {
		t.Fatalf("query failed: %d %v", status, err)
	}
	window, err := metricdata.Decode([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || window.Metrics[0].Name != promQLResultMetricName ||
		len(window.Metrics[0].Samples) != 13 ||
		!window.Metrics[0].Samples[0].Timestamp.Equal(time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("expression window changed: calls=%d %+v", calls.Load(), window)
	}
}

func TestPromQLJSONInstantAndErrorResponses(t *testing.T) {
	tests := []struct {
		name, response string
		status         int
		samples        int
	}{
		{"vector", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"code":"200"},"value":[1792591200.125,"1.25"]}]}}`, 200, 1},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1792591200,"2"]}}`, 200, 1},
		{"empty", `{"status":"success","data":{"resultType":"matrix","result":[]}}`, 200, 0},
		{"query error", `{"status":"error","errorType":"bad_data","error":"invalid query"}`, 400, 0},
		{"invalid JSON", `{broken`, 502, 0},
		{"non-finite", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792591200,"NaN"]}]}}`, 502, 0},
		{"native histogram", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792591200,"1"],"histogram":[1792591200,{}]}]}}`, 502, 0},
		{"bad tuple", `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1792591200]]}]}}`, 502, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/query" || r.URL.Query().Has("format") {
					t.Errorf("incorrect instant query: %s", r.URL)
				}
				fmt.Fprint(w, test.response)
			}))
			defer server.Close()
			body, status, err := collectPromQLMetrics(context.Background(), server.URL,
				url.Values{"query": {"sum(up)"}, "format": {"json"}}, promQLWindowCredential{}, server.Client())
			if status != test.status || (err == nil) != (status == http.StatusOK) {
				t.Fatalf("unexpected result: status=%d error=%v body=%s", status, err, body)
			}
			if status == http.StatusOK {
				window, err := metricdata.Decode([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for _, series := range window.Metrics {
					count += len(series.Samples)
				}
				if count != test.samples {
					t.Fatalf("incorrect instant result count: %d", count)
				}
			}
		})
	}
}

func TestPromQLFormatValidation(t *testing.T) {
	oldLogger := logger
	logger = slogger.NewCliLogger(io.Discard)
	t.Cleanup(func() { logger = oldLogger })
	for _, query := range []string{"format=", "format=xml", "format=json&format=json"} {
		req := httptest.NewRequest(http.MethodGet, "/probe/metrics/promql?query=up&"+query, nil)
		w := httptest.NewRecorder()
		probeMetricsPromQLHandler(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid format returned %d: %s", w.Code, w.Body.String())
		}
	}
	body := promQLWindowFixture(t, "", false)
	points, err := parsePromQLResponse(body, false)
	if err != nil || len(points) != 2 {
		t.Fatalf("text parser must retain one point per series: %d %v", len(points), err)
	}
}

func TestPromQLSourceTimeMatchingRejectsInvalidEvaluationPoint(t *testing.T) {
	for _, name := range []string{"missing", "mismatched evaluation time"} {
		t.Run(name, func(t *testing.T) {
			samples, err := parsePromQLResponse(promQLWindowFixture(t, "requests_total", false), true)
			if err != nil {
				t.Fatal(err)
			}
			timestamps, err := parsePromQLResponse(promQLWindowFixture(t, "requests_total", true), true)
			if err != nil {
				t.Fatal(err)
			}
			if name == "missing" {
				timestamps = timestamps[1:]
			} else {
				timestamps[0].timestampMillis++
			}
			if err := applyPromQLSourceTimestamps(samples, timestamps, "azure_metrics_exporter_source_name"); err == nil {
				t.Fatal("missing or mismatched source timestamps must fail, not produce a partial window")
			}
			for _, timestamp := range timestamps {
				if timestamp.labels["azure_metrics_exporter_source_name"] != "requests_total" {
					t.Fatal("source timestamp matching mutated shared series labels")
				}
			}
		})
	}
}

func TestPromQLJSONQueryDelayAndFormatStayExporterLocal(t *testing.T) {
	now := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	params := url.Values{"query": {"sum(up)"}, "format": {"json"}, "query_delay": {"20m"}}
	prepared, err := preparePromQLQueryParams(params, now)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Has("format") || prepared.Has("query_delay") ||
		prepared.Get("time") != "2026-10-21T13:40:00Z" ||
		!params.Has("format") || !params.Has("query_delay") {
		t.Fatalf("incorrect prepared parameters: %v original=%v", prepared, params)
	}
	params.Set("start", "2026-10-21T12:00:00Z")
	if _, err := preparePromQLQueryParams(params, now); err == nil {
		t.Fatal("JSON mode must not allow query_delay together with a range query")
	}
}
