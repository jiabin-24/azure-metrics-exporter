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
	if params["start"][0] != "2026-10-21T15:59:15Z" ||
		params["end"][0] != "2026-10-21T16:00:15Z" || params["step"][0] != "15s" ||
		params["query"][0] != job.Params["query"][0] || params["format"][0] != "json" {
		t.Fatalf("incorrect dynamic range or other parameters: %v", params)
	}
	next := job.paramsAt(now.Add(time.Minute))
	if next["start"][0] != params["end"][0] || next["end"][0] != "2026-10-21T16:01:15Z" {
		t.Fatalf("range did not move by one minute: %v -> %v", params, next)
	}
	if _, exists := job.Params["start"]; exists || len(job.Params) != 2 ||
		params["start"][0] != "2026-10-21T15:59:15Z" {
		t.Fatal("evaluating another scrape mutated config or a previous batch")
	}
}

func TestRollingRangeCustomWindowDelayAndStep(t *testing.T) {
	cfg, err := ParseConfig([]byte("global:\n  honor_timestamps: false\n" + strings.Replace(rollingConfig, "rolling_range: {}",
		"rolling_range:\n    window: 3m\n    step: 30s\n    delay: 20m", 1)))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	now := time.Date(2026, 10, 22, 0, 10, 24, 0, time.UTC)
	params := job.paramsAt(now)
	if params["start"][0] != "2026-10-21T23:47:00Z" ||
		params["end"][0] != "2026-10-21T23:50:00Z" || params["step"][0] != "30s" {
		t.Fatalf("delay or custom range was not applied: %v", params)
	}
	copy := job
	copy.Params = params
	u, err := url.Parse(copy.targetURL("localhost:8080"))
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	if query.Get("honor_timestamps") != "false" {
		t.Fatalf("timestamp policy not forwarded with delayed range: %s", u)
	}
	query.Del("honor_timestamps")
	if !reflect.DeepEqual(map[string][]string(query), params) {
		t.Fatalf("dynamic parameters did not survive URL encoding: %v, %v", u, err)
	}
}

func TestRollingRangeGlobalInheritanceAndOverrides(t *testing.T) {
	const global = "global:\n  rolling_range: {window: 3m, step: 30s, delay: 20m}\n"
	tests := []struct {
		name   string
		job    string
		window time.Duration
		step   time.Duration
		delay  time.Duration
	}{
		{"omitted", "", 3 * time.Minute, 30 * time.Second, 20 * time.Minute},
		{"empty", "  rolling_range: {}\n", 3 * time.Minute, 30 * time.Second, 20 * time.Minute},
		{"window", "  rolling_range: {window: 5m}\n", 5 * time.Minute, 30 * time.Second, 20 * time.Minute},
		{"step", "  rolling_range: {step: 15s}\n", 3 * time.Minute, 15 * time.Second, 20 * time.Minute},
		{"delay", "  rolling_range: {delay: 5m}\n", 3 * time.Minute, 30 * time.Second, 5 * time.Minute},
		{"zero delay", "  rolling_range: {delay: 0s}\n", 3 * time.Minute, 30 * time.Second, 0},
		{"all", "  rolling_range: {window: 1m, step: 15s, delay: 0s}\n", time.Minute, 15 * time.Second, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := global + strings.Replace(rollingConfig, "  rolling_range: {}\n", tt.job, 1)
			input += strings.Replace(strings.TrimPrefix(rollingConfig, "\nscrape_configs:\n"),
				"job_name: rolling", "job_name: sibling", 1)
			cfg, err := ParseConfig([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			rolling := cfg.Jobs[0].RollingRange
			if rolling == nil || rolling.Window != Duration(tt.window) || rolling.Step != Duration(tt.step) ||
				rolling.Delay != NonNegativeDuration(tt.delay) {
				t.Fatalf("incorrect effective rolling range: %+v", rolling)
			}
			for _, other := range []*RollingRange{cfg.Global.RollingRange, cfg.Jobs[1].RollingRange} {
				if other.Window != Duration(3*time.Minute) || other.Step != Duration(30*time.Second) ||
					other.Delay != NonNegativeDuration(20*time.Minute) || other == rolling {
					t.Fatalf("job override changed global or sibling range: %+v", other)
				}
			}
			now := time.Unix(3600, 0)
			params := cfg.Jobs[0].paramsAt(now)
			end := previousStep(now.Add(-tt.delay), tt.step)
			if params["start"][0] != end.Add(-tt.window).Format(time.RFC3339Nano) ||
				params["end"][0] != end.Format(time.RFC3339Nano) || params["step"][0] != tt.step.String() {
				t.Fatalf("effective range not used in query: %v", params)
			}
		})
	}
}

func TestRollingRangeGlobalDefaultsAndValidation(t *testing.T) {
	input := "global:\n  rolling_range: {}\n" + strings.Replace(rollingConfig, "  rolling_range: {}\n", "", 1)
	cfg, err := ParseConfig([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	for _, rolling := range []*RollingRange{cfg.Global.RollingRange, cfg.Jobs[0].RollingRange} {
		if rolling == nil || rolling.Window != Duration(time.Minute) ||
			rolling.Step != Duration(15*time.Second) || rolling.Delay != 0 {
			t.Fatalf("incorrect global rolling defaults: %+v", rolling)
		}
	}
	for _, overrides := range []string{"{window: 5s}", "{step: 2m}"} {
		input := "global:\n  rolling_range: {}\n" +
			strings.Replace(rollingConfig, "rolling_range: {}", "rolling_range: "+overrides, 1)
		if _, err := ParseConfig([]byte(input)); err == nil || !strings.Contains(err.Error(), "step must not exceed window") {
			t.Fatalf("invalid merged range accepted: %v", err)
		}
	}
	input = strings.Replace(input, "/probe/metrics/promql", "/metrics", 1)
	if _, err := ParseConfig([]byte(input)); err == nil || !strings.Contains(err.Error(), "rolling_range requires") {
		t.Fatalf("inherited range accepted on unsupported endpoint: %v", err)
	}
}

func TestRollingRangeDisabled(t *testing.T) {
	for _, global := range []string{"", "global:\n  rolling_range: {delay: 20m}\n"} {
		for _, path := range []string{"/probe/metrics/promql", "/probe/metrics/resource", "/metrics"} {
			input := strings.Replace(rollingConfig, "rolling_range: {}", "rolling_range: {enabled: false}", 1)
			input = strings.Replace(input, "/probe/metrics/promql", path, 1)
			input = strings.Replace(input, "format: [json]", "format: [json]\n    query_delay: [20m]", 1)
			cfg, err := ParseConfig([]byte(global + input))
			if err != nil {
				t.Fatal(err)
			}
			job := cfg.Jobs[0]
			if job.RollingRange != nil || !reflect.DeepEqual(job.paramsAt(time.Now()), job.Params) {
				t.Fatalf("disabled rolling range changed parameters: %+v", job)
			}
		}
	}
	input := "global:\n  rolling_range: {enabled: false, delay: 20m}\n" + rollingConfig
	cfg, err := ParseConfig([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jobs[0].RollingRange != nil {
		t.Fatal("global enabled=false was not inherited")
	}
	cfg, err = ParseConfig([]byte(strings.Replace(input, "  rolling_range: {}", "  rolling_range: {enabled: true}", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Jobs[0].RollingRange == nil || cfg.Jobs[0].RollingRange.Delay != NonNegativeDuration(20*time.Minute) {
		t.Fatal("job could not re-enable the global range")
	}
}

func TestPreviousStep(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		step time.Duration
		want time.Time
	}{
		{"fractional second", time.Unix(24, 123456789), 15 * time.Second, time.Unix(15, 0)},
		{"exact boundary", time.Unix(30, 0), 15 * time.Second, time.Unix(15, 0)},
		{"just after boundary", time.Unix(30, 1), 15 * time.Second, time.Unix(30, 0)},
		{"just before boundary", time.Unix(30, -1), 15 * time.Second, time.Unix(15, 0)},
		{"epoch alignment", time.Unix(100, 0), 7 * time.Second, time.Unix(98, 0)},
		{"milliseconds", time.Unix(0, 17500000), 5 * time.Millisecond, time.Unix(0, 15000000)},
		{"exact millisecond", time.Unix(0, 15000000), 5 * time.Millisecond, time.Unix(0, 10000000)},
		{"before epoch", time.Unix(-1, 123456789), 15 * time.Second, time.Unix(-15, 0)},
		{"epoch boundary", time.Unix(0, 0), 15 * time.Second, time.Unix(-15, 0)},
		{"timezone", time.Unix(24, 0).In(time.FixedZone("UTC+8", 8*60*60)), 15 * time.Second, time.Unix(15, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := previousStep(tt.now, tt.step)
			if !got.Equal(tt.want) || got.Location() != time.UTC {
				t.Fatalf("previousStep(%v, %v) = %v, want %v in UTC", tt.now, tt.step, got, tt.want)
			}
			if !got.Before(tt.now) || got.Add(tt.step).Before(tt.now) ||
				got.UnixMilli()%tt.step.Milliseconds() != 0 || got.Nanosecond()%int(time.Millisecond) != 0 {
				t.Fatalf("result is not the closest aligned step strictly before now: %v", got)
			}
		})
	}
}

func TestRollingRangeAlignedBounds(t *testing.T) {
	tests := []struct {
		name   string
		window time.Duration
		step   time.Duration
		delay  time.Duration
		now    time.Time
		start  time.Time
		end    time.Time
	}{
		{"exact boundary", time.Minute, 15 * time.Second, 0, time.Unix(90, 0), time.Unix(15, 0), time.Unix(75, 0)},
		{"expanded window", 61 * time.Second, 15 * time.Second, 0, time.Unix(91, 0), time.Unix(15, 0), time.Unix(90, 0)},
		{"delay before alignment", time.Minute, 15 * time.Second, 7 * time.Second, time.Unix(96, 0), time.Unix(15, 0), time.Unix(75, 0)},
		{"delayed boundary", time.Minute, 15 * time.Second, 7 * time.Second, time.Unix(97, 0), time.Unix(15, 0), time.Unix(75, 0)},
		{"millisecond window", 11 * time.Millisecond, 5 * time.Millisecond, time.Millisecond, time.Unix(0, 22500000), time.Unix(0, 5000000), time.Unix(0, 20000000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := Job{
				Params: map[string][]string{"query": {"up"}},
				RollingRange: &RollingRange{
					Window: Duration(tt.window), Step: Duration(tt.step), Delay: NonNegativeDuration(tt.delay),
				},
			}
			params := job.paramsAt(tt.now)
			start, startErr := time.Parse(time.RFC3339Nano, params["start"][0])
			end, endErr := time.Parse(time.RFC3339Nano, params["end"][0])
			if startErr != nil || endErr != nil || !start.Equal(tt.start) || !end.Equal(tt.end) {
				t.Fatalf("incorrect aligned range: %v (parse errors: %v, %v)", params, startErr, endErr)
			}
			if start.UnixMilli()%tt.step.Milliseconds() != 0 || end.UnixMilli()%tt.step.Milliseconds() != 0 ||
				end.Sub(start) < tt.window || end.Sub(start)-tt.window >= tt.step ||
				!end.Before(tt.now.Add(-tt.delay)) {
				t.Fatalf("range does not satisfy step alignment and window bounds: %v", params)
			}
			for point := start; !point.After(end); point = point.Add(tt.step) {
				if point.UnixMilli()%tt.step.Milliseconds() != 0 {
					t.Fatalf("unaligned output timestamp: %v", point)
				}
			}
		})
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
			input = "global:\n  rolling_range: " + rolling + "\n" +
				strings.Replace(rollingConfig, "  rolling_range: {}\n", "", 1)
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatal("invalid global rolling range was accepted")
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
				input = "global:\n  rolling_range: {}\n" + strings.Replace(input, "  rolling_range: {}\n", "", 1)
				if _, err := ParseConfig([]byte(input)); err == nil || !strings.Contains(err.Error(), "params."+key) {
					t.Fatalf("conflicting parameter with inherited range was accepted: %v", err)
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
			start.UnixNano()%int64(15*time.Second) != 0 || end.UnixNano()%int64(15*time.Second) != 0 ||
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
	if secondEnd.Before(firstEnd) {
		t.Fatalf("range moved backwards across scheduled scrapes: %v / %v", got[0].Params, got[1].Params)
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
