package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWorkspaceConfigValidation(t *testing.T) {
	base := "scrape_configs:\n- job_name: test\n  metrics_path: /probe/metrics/promql\n  static_configs:\n  - targets: [exporter:8080]\n"
	for name, extra := range map[string]string{
		"single":           "  workspaces: [a]\n",
		"multiple":         "  workspaces: [a, b]\n",
		"missing":          "",
		"empty":            "  workspaces: []\n",
		"duplicate":        "  workspaces: [a, a]\n",
		"invalid":          "  workspaces: ['https://workspace']\n",
		"params workspace": "  workspaces: [a]\n  params:\n    workspace: [b]\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(base + extra))
			if (name == "single" || name == "multiple") != (err == nil) {
				t.Fatalf("unexpected validation result: %v", err)
			}
			if err == nil && cfg.Cluster != nil {
				t.Fatal("standalone configuration must not enable gossip")
			}
		})
	}
	valid := base + "  workspaces: [a]\n"
	if _, err := ParseConfig([]byte(valid + "sharding: {total: 2, index: 0}\n")); err == nil {
		t.Fatal("removed static sharding configuration must fail parsing")
	}
	if _, err := ParseConfig([]byte(strings.Replace(valid, "/probe/metrics/promql", "/metrics", 1))); err == nil {
		t.Fatal("workspaces accepted on a non-PromQL job")
	}
}

func TestTasksExpandEveryWorkspaceAndTarget(t *testing.T) {
	job := testJob("exporter:8080")
	job.StaticConfigs[0].Targets = []string{"exporter:8080", "other:8080"}
	job.Workspaces = []string{"a", "b"}
	platform := testJob("platform:8080")
	platform.Name, platform.MetricsPath = "platform", "/probe/metrics/resource"
	cfg := &Config{Jobs: []Job{job, platform}}
	expected := map[[3]string]bool{
		{job.Name, "a", "exporter:8080"}:     false,
		{job.Name, "b", "exporter:8080"}:     false,
		{job.Name, "a", "other:8080"}:        false,
		{job.Name, "b", "other:8080"}:        false,
		{platform.Name, "", "platform:8080"}: false,
	}
	tasks := cfg.Tasks()
	if len(tasks) != len(expected) {
		t.Fatalf("got %d tasks; want %d", len(tasks), len(expected))
	}
	ids := make(map[string]bool)
	for _, task := range tasks {
		key := [3]string{task.Job.Name, workspaceFor(task.Job), task.Target}
		if seen, exists := expected[key]; !exists || seen || ids[task.ID] ||
			task.ID != taskID(key[0], key[1], key[2]) {
			t.Fatalf("unexpected, duplicate or incorrectly identified task: %+v", task)
		}
		expected[key], ids[task.ID] = true, true
	}
}

func TestTaskIdentityIgnoresOrderingAndParamsAreIsolated(t *testing.T) {
	job := testJob("exporter:8080")
	job.Workspaces = []string{"a", "b"}
	job.StaticConfigs[0].Targets = []string{"exporter:8080", "other:8080"}
	cfg := &Config{Jobs: []Job{job}}
	tasks := cfg.Tasks()
	ids := make(map[string]bool)
	for _, task := range tasks {
		ids[task.ID] = true
		if len(task.Job.Params["workspace"]) != 1 || !slices.Contains(job.Workspaces, task.Job.Params["workspace"][0]) {
			t.Fatal("expanded task must select exactly one workspace")
		}
	}
	slices.Reverse(job.Workspaces)
	slices.Reverse(job.StaticConfigs[0].Targets)
	for _, task := range cfg.Tasks() {
		if !ids[task.ID] {
			t.Fatal("configuration reordering changed task identity")
		}
	}
	tasks[0].Job.Params["workspace"] = []string{"changed"}
	if _, exists := job.Params["workspace"]; exists || tasks[1].Job.Params["workspace"][0] == "changed" {
		t.Fatal("workspace expansion shares mutable parameter maps")
	}
}

func TestTwoMemberlistInstancesCollectDisjointWorkspaces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Query()["workspace"]) != 1 {
			t.Error("request did not select exactly one workspace")
		}
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	target := strings.TrimPrefix(server.URL, "http://")
	job := testJob(target)
	job.Interval = Duration(time.Hour)
	for i := range 20 {
		job.Workspaces = append(job.Workspaces, fmt.Sprintf("workspace-%d", i))
	}
	type delivery struct {
		index int
		batch Batch
	}
	deliveries := make(chan delivery, len(job.Workspaces)*2)
	cfg := &Config{Jobs: []Job{job}}
	first := testCluster(t, cfg, "node-0")
	second := testCluster(t, cfg, "node-1", first.list.LocalNode().Address())
	waitFor(t, "two-node membership", func() bool {
		return first.Ready() && second.Ready() && len(first.Members()) == 2 && len(second.Members()) == 2
	})
	clusters := []*Cluster{first, second}
	for index := range 2 {
		stop := startTestWorker(t, cfg, clusters[index], func(ctx context.Context, batch Batch) error {
			select {
			case deliveries <- delivery{index, batch}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		defer stop()
	}
	seen := make(map[string]bool)
	for range job.Workspaces {
		select {
		case got := <-deliveries:
			if seen[got.batch.TaskID] || got.batch.Workspace == "" ||
				got.batch.TaskID != taskID(job.Name, got.batch.Workspace, target) ||
				taskOwner(got.batch.TaskID, first.Members()) != clusters[got.index].Name() {
				t.Fatalf("incorrect delivery provenance or duplicate task: %+v", got)
			}
			seen[got.batch.TaskID] = true
		case <-time.After(3 * time.Second):
			t.Fatal("memberlist tasks were not all collected")
		}
	}
	select {
	case got := <-deliveries:
		t.Fatalf("duplicate delivery after membership converged: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWorkspaceEventProvenanceAndPartitionIsolation(t *testing.T) {
	batch := eventTestBatch()
	batch.TaskID = taskID("controlplane", batch.Workspace, "exporter")
	first, err := exportWindows(batch)
	if err != nil {
		t.Fatal(err)
	}
	batch.Workspace = "workspace-b"
	second, err := exportWindows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].partitionKey == second[0].partitionKey || !reflect.DeepEqual(first[0].Metrics, second[0].Metrics) {
		t.Fatal("workspace isolation changed samples or reused a partition key")
	}
	bodies, err := splitWindow(first[0], eventBodyLimit)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(bodies[0], &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["workspace"]) != `"workspace-a"` || string(fields["taskId"]) != `"`+batch.TaskID+`"` {
		t.Fatalf("event provenance missing: %s", bodies[0])
	}
}

func TestFailedWorkspaceDoesNotBlockHealthyWorkspace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("workspace") == "bad" {
			http.Error(w, "workspace unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	job.Workspaces = []string{"bad", "healthy"}
	job.Interval = Duration(time.Hour)
	cfg := &Config{Jobs: []Job{job}}
	delivered := make(chan Batch, 2)
	stop := startTestWorker(t, cfg, nil, func(_ context.Context, batch Batch) error {
		delivered <- batch
		return nil
	})
	defer stop()
	select {
	case batch := <-delivered:
		if batch.Workspace != "healthy" {
			t.Fatalf("failed workspace reached the sink: %+v", batch)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed workspace blocked the healthy workspace")
	}
}
