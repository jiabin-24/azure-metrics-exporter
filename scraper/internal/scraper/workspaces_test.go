package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveAllWorkspacesPerTarget(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	server := func(calls *atomic.Int32, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.Method != http.MethodGet || r.URL.Path != "/probe/metrics/promql/workspaces" ||
				r.URL.RawQuery != "" || r.Header.Get("Accept") != "application/json" {
				t.Errorf("unexpected discovery request: %s %s", r.Method, r.URL)
			}
			fmt.Fprint(w, body)
		}))
	}
	first := server(&firstCalls, `{"workspaces":["b","a"]}`)
	second := server(&secondCalls, `{"workspaces":["c"]}`)
	defer first.Close()
	defer second.Close()
	firstTarget, secondTarget := strings.TrimPrefix(first.URL, "http://"), strings.TrimPrefix(second.URL, "http://")
	job := testJob(firstTarget)
	job.StaticConfigs[0].Targets = []string{firstTarget, secondTarget}
	other := job
	other.Name = "other"
	platform := testJob("must-not-be-contacted:8080")
	platform.Name, platform.MetricsPath = "platform", "/probe/metrics/resource"
	cfg := &Config{Jobs: []Job{job, other, platform}}
	worker := New(cfg, quietLogger(), nil)
	defer worker.Client.CloseIdleConnections()
	if err := worker.ResolveWorkspaces(t.Context()); err != nil {
		t.Fatal(err)
	}
	tasks := cfg.Tasks()
	if len(tasks) != 7 || firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("unexpected expansion/cache: tasks=%d calls=%d/%d", len(tasks), firstCalls.Load(), secondCalls.Load())
	}
	for _, expanded := range cfg.Jobs {
		target := expanded.StaticConfigs[0].Targets[0]
		var want []string
		if target == firstTarget {
			want = []string{"a", "b"}
		} else if target == secondTarget {
			want = []string{"c"}
		}
		if !reflect.DeepEqual(expanded.workspaces, want) || expanded.StaticConfigs[0].Labels["env"] != "test" {
			t.Fatalf("incorrect workspace list or lost labels: %+v", expanded)
		}
	}
	for _, task := range tasks {
		workspace := workspaceFor(task.Job)
		if (workspace == "") != (task.Job.Name == "platform") || workspace == "*" ||
			task.ID != taskID(task.Job.Name, workspace, task.Target) {
			t.Fatalf("incorrect workspace in expanded task: %+v", task)
		}
	}
	if err := worker.ResolveWorkspaces(t.Context()); err != nil ||
		firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("resolved configuration should not rediscover: %v", err)
	}
}

func TestWorkspaceDiscoveryRejectsInvalidResponses(t *testing.T) {
	for _, test := range []struct {
		name, body string
		status     int
	}{
		{"unavailable", "not configured", http.StatusServiceUnavailable},
		{"missing endpoint", "not found", http.StatusNotFound},
		{"invalid JSON", "{", http.StatusOK},
		{"trailing JSON", `{"workspaces":["a"]}{}`, http.StatusOK},
		{"missing list", `{}`, http.StatusOK},
		{"empty list", `{"workspaces":[]}`, http.StatusOK},
		{"null list", `{"workspaces":null}`, http.StatusOK},
		{"invalid alias", `{"workspaces":["https://a"]}`, http.StatusOK},
		{"wildcard", `{"workspaces":["*"]}`, http.StatusOK},
		{"duplicate alias", `{"workspaces":["a","a"]}`, http.StatusOK},
		{"oversized", strings.Repeat(" ", MaxBodyBytes+1), http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			worker := New(&Config{}, quietLogger(), nil)
			defer worker.Client.CloseIdleConnections()
			if _, err := worker.discoverWorkspaces(t.Context(), server.URL, time.Second); err == nil {
				t.Fatal("invalid workspace inventory was accepted")
			}
		})
	}
}

func TestWorkspaceDiscoveryTimeoutCancellationAndRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	worker := New(&Config{}, quietLogger(), nil)
	defer worker.Client.CloseIdleConnections()
	if _, err := worker.discoverWorkspaces(t.Context(), server.URL, 20*time.Millisecond); err == nil {
		t.Fatal("discovery did not time out")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := worker.discoverWorkspaces(ctx, server.URL, time.Second); err == nil {
		t.Fatal("discovery ignored cancellation")
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, server.URL, http.StatusFound)
	}))
	defer redirect.Close()
	if _, err := worker.discoverWorkspaces(t.Context(), redirect.URL, time.Second); err == nil {
		t.Fatal("discovery accepted a redirect")
	}
}

func TestWorkspaceDiscoveryFailurePreservesConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"workspaces":["a"]}`)
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	other := job
	other.Name = "failing"
	other.StaticConfigs = []StaticConfig{{Targets: []string{"invalid target"}}}
	cfg := &Config{Jobs: []Job{job, other}}
	before := append([]Job(nil), cfg.Jobs...)
	worker := New(cfg, quietLogger(), nil)
	defer worker.Client.CloseIdleConnections()
	if err := worker.ResolveWorkspaces(t.Context()); err == nil {
		t.Fatal("discovery failure was not surfaced")
	}
	if !reflect.DeepEqual(cfg.Jobs, before) {
		t.Fatal("failed discovery left a partially expanded configuration")
	}
}
