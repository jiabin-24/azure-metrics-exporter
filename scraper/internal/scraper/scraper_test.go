package scraper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testJob(target string) Job {
	return Job{
		Name: "test", Scheme: "http", MetricsPath: "/probe/metrics/promql",
		Interval: Duration(20 * time.Millisecond), Timeout: Duration(time.Second),
		Params:        map[string][]string{"query": {"up", "other"}, "query_delay": {"20m"}},
		StaticConfigs: []StaticConfig{{Targets: []string{target}, Labels: map[string]string{"env": "test"}}},
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestScrapePreservesPayloadAndMetadata(t *testing.T) {
	const payload = "# HELP up Upstream state\n# TYPE up gauge\nup{cluster=\"prod\"} 1 1792404000123\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/probe/metrics/promql" || len(r.URL.Query()["query"]) != 2 ||
			r.URL.Query().Get("query_delay") != "20m" || r.Header.Get("Accept") == "" {
			t.Errorf("unexpected request: %s", r.URL)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, payload)
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	var got Batch
	s := New(&Config{}, quietLogger(), func(_ context.Context, batch Batch) error {
		got = batch
		return nil
	})
	defer s.Client.CloseIdleConnections()
	start := time.Now()
	if err := s.Scrape(context.Background(), job, target, job.StaticConfigs[0].Labels); err != nil {
		t.Fatal(err)
	}
	if string(got.Metrics) != payload || got.Job != job.Name || got.Target != target ||
		got.Labels["env"] != "test" || got.Families != 1 || got.ScrapedAt.Before(start) ||
		got.ContentType != "text/plain; version=0.0.4" {
		t.Fatalf("unexpected batch: %+v", got)
	}
}

func TestScrapeFailuresDoNotReachSink(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{"HTTP error", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}, "HTTP 502"},
		{"redirect", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "/login")
			w.WriteHeader(http.StatusFound)
		}, "HTTP 302"},
		{"invalid text", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, "<html>not metrics</html>")
		}, "invalid Prometheus"},
		{"too large", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, strings.Repeat("x", MaxBodyBytes+1))
		}, "exceeds"},
		{"timeout", func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}, "deadline exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()
			target := strings.TrimPrefix(server.URL, "http://")
			job := testJob(target)
			if test.name == "timeout" {
				job.Timeout = Duration(20 * time.Millisecond)
			}
			s := New(&Config{}, quietLogger(), func(context.Context, Batch) error {
				t.Error("failed scrape must not reach sink")
				return nil
			})
			defer s.Client.CloseIdleConnections()
			if err := s.Scrape(context.Background(), job, target, nil); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %v", test.want, err)
			}
		})
	}
}

func TestSinkFailureIsReturned(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	want := errors.New("sink unavailable")
	s := New(&Config{}, quietLogger(), func(context.Context, Batch) error { return want })
	defer s.Client.CloseIdleConnections()
	if err := s.Scrape(context.Background(), testJob(target), target, nil); !errors.Is(err, want) {
		t.Fatalf("expected sink error, got %v", err)
	}
}

func TestEmptyAndMaximumSizeResponses(t *testing.T) {
	for _, size := range []int{0, MaxBodyBytes} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := ""
			if size > 0 {
				payload = "#" + strings.Repeat("x", size-2) + "\n"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, payload)
			}))
			defer server.Close()
			target := strings.TrimPrefix(server.URL, "http://")
			delivered := false
			s := New(&Config{}, quietLogger(), func(_ context.Context, batch Batch) error {
				delivered = true
				if len(batch.Metrics) != size || batch.Families != 0 {
					t.Fatalf("unexpected empty/comment-only batch: bytes=%d families=%d", len(batch.Metrics), batch.Families)
				}
				return nil
			})
			defer s.Client.CloseIdleConnections()
			if err := s.Scrape(context.Background(), testJob(target), target, nil); err != nil {
				t.Fatal(err)
			}
			if !delivered {
				t.Fatal("valid empty/comment-only result was not delivered")
			}
		})
	}
}

func TestRunRepeatsAfterFailureAndStops(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	batches := make(chan Batch, 10)
	var logs bytes.Buffer
	s := New(&Config{Jobs: []Job{job}}, slog.New(slog.NewTextHandler(&logs, nil)),
		func(_ context.Context, batch Batch) error {
			batches <- batch
			return nil
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for range 2 {
		select {
		case <-batches:
		case <-time.After(3 * time.Second):
			t.Fatal("scheduled scrape did not repeat after failure")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scraper did not stop")
	}
	if !strings.Contains(logs.String(), "scrape failed") || !strings.Contains(logs.String(), "HTTP 503") {
		t.Fatalf("failure was not logged: %s", &logs)
	}
}

func TestTargetsRunIndependentlyAndCancelRequests(t *testing.T) {
	started := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		started <- r.URL.Path
		<-r.Context().Done()
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	first, second := testJob(target), testJob(target)
	first.MetricsPath, second.MetricsPath = "/first", "/second"
	first.Interval, second.Interval = Duration(time.Hour), Duration(time.Hour)
	first.Timeout, second.Timeout = Duration(time.Hour), Duration(time.Hour)
	s := New(&Config{Jobs: []Job{first, second}}, quietLogger(), func(context.Context, Batch) error {
		t.Error("cancelled request reached sink")
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("one job blocked another")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel in-flight requests")
	}
}

func TestUnconfiguredSinkExplicitlyLogsDiscard(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	err := DiscardSink(logger)(context.Background(), Batch{Job: "test", Metrics: []byte("secret_payload")})
	if err != nil || !strings.Contains(logs.String(), "discarding batch") || strings.Contains(logs.String(), "secret_payload") {
		t.Fatalf("unexpected discard sink result: %v, %s", err, &logs)
	}
}
