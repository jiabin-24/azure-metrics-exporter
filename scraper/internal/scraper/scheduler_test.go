package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type schedulerTransport func(*http.Request) (*http.Response, error)

func (f schedulerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func schedulerResponse(t *testing.T, empty bool) *http.Response {
	t.Helper()
	window := eventTestBatch().Window
	if empty {
		window.Metrics = nil
	}
	body, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func schedulerJob(name string, interval time.Duration) Job {
	job := testJob("exporter:8080")
	job.Name = name
	job.Interval, job.Timeout = Duration(interval), Duration(time.Hour)
	job.Params = map[string][]string{"query": {name}, "format": {"json"}}
	return job
}

func startBufferedWorker(t *testing.T, jobs []Job, transport schedulerTransport, sink BatchSink, logger *slog.Logger) func() {
	t.Helper()
	s := New(&Config{Jobs: jobs, EventHub: &EventHubConfig{}}, logger, nil)
	s.Client.Transport, s.BatchSink = transport, sink
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

func TestBufferedSchedulerFlushesWhenAllTasksComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		start := time.Now()
		jobs := []Job{schedulerJob("first", time.Minute), schedulerJob("second", time.Minute)}
		startBufferedWorker(t, jobs, func(req *http.Request) (*http.Response, error) {
			delay := time.Second
			if req.URL.Query().Get("query") == "second" {
				delay = 20 * time.Second
			}
			time.Sleep(delay)
			return schedulerResponse(t, false), nil
		}, eventHubBatchSink(sender, quietLogger()), quietLogger())
		synctest.Wait()
		time.Sleep(19 * time.Second)
		synctest.Wait()
		if len(sender.bodies) != 0 {
			t.Fatal("sent before all tasks completed")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(sender.bodies) != 1 || time.Since(start) != 20*time.Second {
			t.Fatalf("all jobs should produce one event immediately, events=%d", len(sender.bodies))
		}
		var got exportWindow
		if err := json.Unmarshal(sender.bodies[0], &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Metrics) != 2 || !got.Metrics[0].Samples[0].Timestamp.Equal(eventTestBatch().Window.Metrics[0].Samples[0].Timestamp) {
			t.Fatal("merge lost metrics or replaced source timestamps")
		}
		time.Sleep(39 * time.Second)
		synctest.Wait()
		if len(sender.bodies) != 1 {
			t.Fatal("early flush changed task cadence or sent duplicate events")
		}
	})
}

func TestBufferedSchedulerRetainsLateResultsAndSkipsRunningTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(map[string]int)
		var mu sync.Mutex
		var deliveries [][]Batch
		jobs := []Job{schedulerJob("fast", time.Minute), schedulerJob("slow", time.Minute)}
		startBufferedWorker(t, jobs, func(req *http.Request) (*http.Response, error) {
			name := req.URL.Query().Get("query")
			mu.Lock()
			calls[name]++
			mu.Unlock()
			delay := time.Second
			if name == "slow" {
				delay = 90 * time.Second
			}
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(delay):
				return schedulerResponse(t, false), nil
			}
		}, func(_ context.Context, batches []Batch) error {
			deliveries = append(deliveries, batches)
			return nil
		}, quietLogger())
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		if len(deliveries) != 1 || len(deliveries[0]) != 1 || deliveries[0][0].Job != "fast" {
			t.Fatalf("interval failed to flush partial results: %+v", deliveries)
		}
		if calls["slow"] != 1 || calls["fast"] != 2 {
			t.Fatalf("slow task overlapped or fast task did not repeat: %v", calls)
		}
		time.Sleep(29 * time.Second)
		synctest.Wait()
		if len(deliveries) != 1 {
			t.Fatal("second batch did not wait for outstanding slow task")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(deliveries) != 2 || len(deliveries[1]) != 2 {
			t.Fatalf("late data was cancelled or not included in the next batch: %+v", deliveries)
		}
		if calls["slow"] != 1 {
			t.Fatal("completed slow task restarted before its next scheduled interval")
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if calls["slow"] != 2 || calls["fast"] != 3 || len(deliveries) != 2 {
			t.Fatalf("next interval or empty flush incorrect: calls=%v deliveries=%d", calls, len(deliveries))
		}
	})
}

func TestBufferedSchedulerCountsEmptyFailuresAndTimeoutsAsCompleted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		sender := &recordingSender{}
		var jobs []Job
		for _, name := range []string{"valid", "empty", "error", "timeout"} {
			jobs = append(jobs, schedulerJob(name, time.Minute))
		}
		jobs[3].Timeout = Duration(3 * time.Second)
		startBufferedWorker(t, jobs, func(req *http.Request) (*http.Response, error) {
			switch req.URL.Query().Get("query") {
			case "error":
				return nil, errors.New("upstream unavailable")
			case "timeout":
				<-req.Context().Done()
				return nil, req.Context().Err()
			case "empty":
				return schedulerResponse(t, true), nil
			default:
				return schedulerResponse(t, false), nil
			}
		}, eventHubBatchSink(sender, logger), logger)
		synctest.Wait()
		if len(sender.bodies) != 0 {
			t.Fatal("flushed before timed-out task finished")
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if len(sender.bodies) != 1 || !strings.Contains(logs.String(), "upstream unavailable") ||
			!strings.Contains(logs.String(), "deadline exceeded") {
			t.Fatalf("completion or error logging incorrect: events=%d logs=%s", len(sender.bodies), &logs)
		}
	})
}

func TestBufferedSchedulerKeepsDifferentIntervalsIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deliveries [][]Batch
		calls := make(map[string]int)
		var mu sync.Mutex
		jobs := []Job{schedulerJob("short", time.Minute), schedulerJob("long", 2*time.Minute)}
		startBufferedWorker(t, jobs, func(req *http.Request) (*http.Response, error) {
			name := req.URL.Query().Get("query")
			mu.Lock()
			calls[name]++
			mu.Unlock()
			if name == "long" {
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				case <-time.After(90 * time.Second):
				}
			}
			return schedulerResponse(t, false), nil
		}, func(_ context.Context, batches []Batch) error {
			deliveries = append(deliveries, batches)
			return nil
		}, quietLogger())
		synctest.Wait()
		if len(deliveries) != 1 || deliveries[0][0].Job != "short" {
			t.Fatal("short group waited for a job with a different interval")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if len(deliveries) != 2 || calls["short"] != 2 || calls["long"] != 1 {
			t.Fatalf("job interval overrides were not honored: %v", calls)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if len(deliveries) != 3 || deliveries[2][0].Job != "long" {
			t.Fatal("long group did not flush on completion")
		}
	})
}

func TestBufferedSchedulerFlushesCollectedDataOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		cancelled := false
		stop := startBufferedWorker(t, []Job{schedulerJob("fast", time.Minute), schedulerJob("blocked", time.Minute)},
			func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Get("query") == "blocked" {
					<-req.Context().Done()
					cancelled = true
					return nil, req.Context().Err()
				}
				return schedulerResponse(t, false), nil
			}, eventHubBatchSink(sender, quietLogger()), quietLogger())
		synctest.Wait()
		if len(sender.bodies) != 0 {
			t.Fatal("sent partial results before shutdown")
		}
		stop()
		if !cancelled || len(sender.bodies) != 1 {
			t.Fatal("shutdown failed to cancel active requests and flush collected data")
		}
	})
}

func TestBufferedSchedulerDoesNotSendEmptyResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sender := &recordingSender{}
		stop := startBufferedWorker(t, []Job{schedulerJob("empty", time.Minute)},
			func(_ *http.Request) (*http.Response, error) {
				return schedulerResponse(t, true), nil
			}, eventHubBatchSink(sender, quietLogger()), quietLogger())
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		stop()
		if len(sender.bodies) != 0 {
			t.Fatal("empty results generated Event Hub messages")
		}
	})
}

func TestBufferedSchedulerLogsShutdownDeliveryFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		stop := startBufferedWorker(t, []Job{schedulerJob("fast", time.Minute), schedulerJob("blocked", time.Minute)},
			func(req *http.Request) (*http.Response, error) {
				if req.URL.Query().Get("query") == "blocked" {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				return schedulerResponse(t, false), nil
			}, eventHubBatchSink(&recordingSender{failAt: 1}, logger), logger)
		synctest.Wait()
		stop()
		if !strings.Contains(logs.String(), "buffered Event Hub delivery failed") ||
			!strings.Contains(logs.String(), "0/1 acknowledged") ||
			strings.Contains(logs.String(), "delivery completed") {
			t.Fatalf("shutdown delivery failure was not reported: %s", &logs)
		}
	})
}

func TestBufferedSchedulerDeliveryDoesNotBlockCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls, sends := 0, 0
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		startBufferedWorker(t, []Job{schedulerJob("job", time.Minute)},
			func(req *http.Request) (*http.Response, error) {
				calls++
				return schedulerResponse(t, false), nil
			}, func(ctx context.Context, batches []Batch) error {
				sends++
				if sends == 1 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(90 * time.Second):
					}
					return fmt.Errorf("send rejected")
				}
				return nil
			}, logger)
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		if calls != 2 || sends != 1 {
			t.Fatalf("delivery blocked subsequent collection: calls=%d sends=%d", calls, sends)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if sends != 2 || !strings.Contains(logs.String(), "buffered Event Hub delivery failed") ||
			!strings.Contains(logs.String(), "send rejected") {
			t.Fatalf("queued delivery or failure logging incorrect: %s", &logs)
		}
	})
}
