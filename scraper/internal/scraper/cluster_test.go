package scraper

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const clusterTestConfig = "scrape_configs:\n- job_name: test\n  static_configs:\n  - targets: [exporter:8080]\n"

func availableGossipAddress(t *testing.T) string {
	t.Helper()
	var lastErr error
	// Windows can reserve different ephemeral port ranges for TCP and UDP.
	for range 100 {
		udp, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := udp.LocalAddr().String()
		tcp, err := net.Listen("tcp", address)
		udp.Close()
		if err == nil {
			tcp.Close()
			return address
		}
		lastErr = err
	}
	t.Fatalf("no loopback port available for both TCP and UDP: %v", lastErr)
	return ""
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testCluster(t *testing.T, cfg *Config, name string, seeds ...string) *Cluster {
	t.Helper()
	copy := *cfg
	copy.Cluster = &ClusterConfig{
		Name: "test", NodeName: name, BindAddress: availableGossipAddress(t),
		Join: seeds, RetryInterval: Duration(100 * time.Millisecond),
	}
	return startTestCluster(t, &copy)
}

func startTestCluster(t *testing.T, cfg *Config) *Cluster {
	t.Helper()
	c, err := NewCluster(t.Context(), cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close test cluster: %v", err)
		}
	})
	return c
}

func startTestWorker(t *testing.T, cfg *Config, cluster *Cluster, sink Sink) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	worker := New(cfg, quietLogger(), sink)
	worker.Cluster = cluster
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()
	return func() { cancel(); <-done }
}

func TestClusterConfigValidation(t *testing.T) {
	for _, test := range []struct {
		yaml  string
		valid bool
	}{
		{"{}", true},
		{"{bind_address: '127.0.0.1:0'}", false},
		{"{bind_address: 'not-an-ip:7946'}", false},
		{"{bind_address: '0.0.0.0:65536'}", false},
		{"{advertise_address: '0.0.0.0:7946'}", false},
		{"{advertise_address: '127.0.0.1:0'}", false},
		{"{advertise_address: '[::1]:7946'}", true},
		{"{join: ['scraper:7946', '[::1]:7946']}", true},
		{"{join: ['scraper']}", false},
		{"{join: ['scraper:1.5']}", false},
		{"{join: ['scraper:0']}", false},
		{"{join: ['scraper:65536']}", false},
		{"{join: ['bad host:7946']}", false},
		{"{node_name: ' '}", false},
		{"{name: ' '}", false},
		{"{retry_interval: '-1s'}", false},
		{"{retry_interval: '0s'}", false},
		{"{retry_interval: '100ms'}", true},
		{"{total: 2}", false},
	} {
		t.Run(test.yaml, func(t *testing.T) {
			cfg, err := ParseConfig([]byte("cluster: " + test.yaml + "\n" + clusterTestConfig))
			if (err == nil) != test.valid {
				t.Fatalf("unexpected validation result: %v", err)
			}
			if err == nil && cfg.Cluster.Name == "" {
				t.Fatal("cluster name must have a default")
			}
		})
	}
}

func TestClusterEnvironment(t *testing.T) {
	for _, test := range []struct {
		env, value string
		valid      bool
	}{
		{"SCRAPER_CLUSTER_NODE_NAME", "node-a", true},
		{"SCRAPER_CLUSTER_BIND_ADDRESS", "127.0.0.1:7947", true},
		{"SCRAPER_CLUSTER_ADVERTISE_ADDRESS", "10.0.0.2:7946", true},
		{"SCRAPER_CLUSTER_JOIN", "one:7946,two:7946", true},
		{"SCRAPER_CLUSTER_NODE_NAME", "", false},
		{"SCRAPER_CLUSTER_BIND_ADDRESS", "host:7946", false},
		{"SCRAPER_CLUSTER_ADVERTISE_ADDRESS", "0.0.0.0:7946", false},
		{"SCRAPER_CLUSTER_JOIN", "one:7946,", false},
	} {
		t.Run(test.env+"/"+test.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "scraper.yaml")
			if err := os.WriteFile(path, []byte("cluster: {}\n"+clusterTestConfig), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv(test.env, test.value)
			cfg, err := LoadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("unexpected environment validation: %v", err)
			}
			if err == nil {
				switch test.env {
				case "SCRAPER_CLUSTER_NODE_NAME":
					if cfg.Cluster.NodeName != test.value {
						t.Fatal("node override not applied")
					}
				case "SCRAPER_CLUSTER_BIND_ADDRESS":
					if cfg.Cluster.BindAddress != test.value {
						t.Fatal("bind override not applied")
					}
				case "SCRAPER_CLUSTER_ADVERTISE_ADDRESS":
					if cfg.Cluster.AdvertiseAddress != test.value {
						t.Fatal("advertise override not applied")
					}
				case "SCRAPER_CLUSTER_JOIN":
					if strings.Join(cfg.Cluster.Join, ",") != test.value {
						t.Fatal("join override not applied")
					}
				}
			}
			if err := os.WriteFile(path, []byte(clusterTestConfig), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil {
				t.Fatal("cluster environment must not silently enable or be ignored in standalone mode")
			}
		})
	}
}

func TestRendezvousOwnershipAndMinimalMovement(t *testing.T) {
	members := []string{"a", "b", "c"}
	before := make(map[string]string)
	for i := range 1000 {
		id := fmt.Sprint(i)
		owner := taskOwner(id, members)
		if !slices.Contains(members, owner) {
			t.Fatal("task lacks a unique valid owner")
		}
		before[id] = owner
	}
	slices.Reverse(members)
	counts := make(map[string]int)
	moved := 0
	for id, previous := range before {
		if taskOwner(id, members) != previous {
			t.Fatal("member ordering changed ownership")
		}
		next := taskOwner(id, append(slices.Clone(members), "d"))
		if next != previous && next != "d" {
			t.Fatal("join moved a task between existing members")
		}
		if next != previous {
			moved++
		}
		remaining := []string{"a", "c"}
		afterLeave := taskOwner(id, remaining)
		if previous != "b" && afterLeave != previous {
			t.Fatal("leave moved an unaffected task")
		}
		counts[previous]++
	}
	if len(counts) != len(members) || moved == 0 || moved == len(before) {
		t.Fatalf("degenerate allocation: counts=%v moved=%d", counts, moved)
	}
	if taskOwner("empty", nil) != "" || taskOwner("single", []string{"only"}) != "only" {
		t.Fatal("incorrect empty/single member allocation")
	}
}

func TestClusterAllowsConfigurationChanges(t *testing.T) {
	cfg, err := ParseConfig([]byte(clusterTestConfig))
	if err != nil {
		t.Fatal(err)
	}
	first := testCluster(t, cfg, "first")
	changed, err := ParseConfig([]byte(strings.Replace(clusterTestConfig, "exporter:8080", "other:8080", 1)))
	if err != nil {
		t.Fatal(err)
	}
	second := testCluster(t, changed, "second", first.list.LocalNode().Address())
	waitFor(t, "membership during configuration changes", func() bool {
		return second.Ready() && len(first.Members()) == 2 && len(second.Members()) == 2
	})
}

func TestClusterJoinRetriesAndGatesTasks(t *testing.T) {
	seed := availableGossipAddress(t)
	requests := make(chan struct{}, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	job.Interval = Duration(time.Hour)
	for i := range 100 {
		job.workspaces = []string{fmt.Sprintf("workspace-%d", i)}
		if taskOwner(taskID(job.Name, job.workspaces[0], job.StaticConfigs[0].Targets[0]),
			[]string{"waiting", "seed"}) == "seed" {
			break
		}
	}
	if taskOwner(taskID(job.Name, job.workspaces[0], job.StaticConfigs[0].Targets[0]),
		[]string{"waiting", "seed"}) != "seed" {
		t.Fatal("could not select task initially owned by seed")
	}
	cfg := &Config{Jobs: []Job{job}}
	waiting := testCluster(t, cfg, "waiting", seed)
	stop := startTestWorker(t, cfg, waiting, DiscardSink(quietLogger()))
	defer stop()
	select {
	case <-requests:
		t.Fatal("unjoined instance started collecting")
	case <-time.After(150 * time.Millisecond):
	}
	if waiting.Ready() {
		t.Fatal("unreachable seed made node ready")
	}
	bootstrapConfig := *cfg
	bootstrapConfig.Cluster = &ClusterConfig{Name: "test", NodeName: "seed", BindAddress: seed}
	bootstrap := startTestCluster(t, &bootstrapConfig)
	waitFor(t, "join retry to discover late seed", func() bool {
		return waiting.Ready() && len(waiting.Members()) == 2 && len(bootstrap.Members()) == 2
	})
	select {
	case <-requests:
		t.Fatal("idle member collected another member's task")
	case <-time.After(100 * time.Millisecond):
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "seed leave", func() bool { return len(waiting.Members()) == 1 })
	select {
	case <-requests:
	case <-time.After(3 * time.Second):
		t.Fatal("idle member stopped scheduling instead of taking over after leave")
	}
}

func TestClusterLabelIsolation(t *testing.T) {
	cfg, err := ParseConfig([]byte(fmt.Sprintf("cluster: {name: first-group, bind_address: '%s'}\n", availableGossipAddress(t)) + clusterTestConfig))
	if err != nil {
		t.Fatal(err)
	}
	first := startTestCluster(t, cfg)
	cfg.Cluster.Name = "second-group"
	cfg.Cluster.BindAddress = availableGossipAddress(t)
	second := startTestCluster(t, cfg)
	if _, err := second.list.Join([]string{first.list.LocalNode().Address()}); err == nil {
		t.Fatal("nodes with different cluster labels merged")
	}
	if len(first.Members()) != 1 || len(second.Members()) != 1 {
		t.Fatal("separate cluster labels shared membership")
	}
}

func TestClusterWorkerHandoffPreservesUnchangedTasks(t *testing.T) {
	started := make(chan string, 100)
	canceled := make(chan string, 100)
	var mu sync.Mutex
	counts := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		workspace := r.URL.Query().Get("workspace")
		mu.Lock()
		counts[workspace]++
		mu.Unlock()
		started <- workspace
		<-r.Context().Done()
		canceled <- workspace
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	job.Timeout, job.Interval = Duration(time.Hour), Duration(time.Hour)
	for i := range 20 {
		job.workspaces = append(job.workspaces, fmt.Sprintf("workspace-%d", i))
	}
	cfg := &Config{Jobs: []Job{job}}
	first := testCluster(t, cfg, "first")
	stopFirst := startTestWorker(t, cfg, first, DiscardSink(quietLogger()))
	defer stopFirst()
	receive := func(channel <-chan string, count int) []string {
		var values []string
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for range count {
			select {
			case value := <-channel:
				values = append(values, value)
			case <-timer.C:
				t.Fatalf("received %d of %d expected lifecycle events", len(values), count)
			}
		}
		return values
	}
	receive(started, len(job.workspaces))
	second := testCluster(t, cfg, "second", first.list.LocalNode().Address())
	waitFor(t, "handoff membership", func() bool {
		return second.Ready() && len(first.Members()) == 2 && len(second.Members()) == 2
	})
	moved := make(map[string]bool)
	for _, task := range cfg.Tasks() {
		if taskOwner(task.ID, first.Members()) == second.Name() {
			moved[task.Job.Params["workspace"][0]] = true
		}
	}
	if len(moved) == 0 || len(moved) == len(job.workspaces) {
		t.Fatal("test requires both retained and moved tasks")
	}
	for _, workspace := range receive(canceled, len(moved)) {
		if !moved[workspace] {
			t.Fatalf("unchanged task was canceled: %s", workspace)
		}
	}
	stopSecond := startTestWorker(t, cfg, second, DiscardSink(quietLogger()))
	defer stopSecond()
	for _, workspace := range receive(started, len(moved)) {
		if !moved[workspace] {
			t.Fatalf("unchanged task was restarted: %s", workspace)
		}
	}
	stopSecond()
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "graceful leave", func() bool { return len(first.Members()) == 1 })
	for _, workspace := range receive(started, len(moved)) {
		if !moved[workspace] {
			t.Fatal("leave restarted unaffected task")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, workspace := range job.workspaces {
		want := 1
		if moved[workspace] {
			want = 3
		}
		if counts[workspace] != want {
			t.Fatalf("task %s started %d times; want %d", workspace, counts[workspace], want)
		}
	}
}

func TestClusterDetectsAbruptFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "up 1\n")
	}))
	defer server.Close()
	job := testJob(strings.TrimPrefix(server.URL, "http://"))
	job.Interval = Duration(time.Hour)
	for i := range 20 {
		job.workspaces = append(job.workspaces, fmt.Sprintf("workspace-%d", i))
	}
	cfg := &Config{Jobs: []Job{job}}
	first := testCluster(t, cfg, "first")
	second := testCluster(t, cfg, "second", first.list.LocalNode().Address())
	waitFor(t, "initial membership", func() bool {
		return second.Ready() && len(first.Members()) == 2 && len(second.Members()) == 2
	})
	delivered := make(chan Batch, 100)
	sink := func(ctx context.Context, batch Batch) error {
		select {
		case delivered <- batch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	stopFirst := startTestWorker(t, cfg, first, sink)
	stopSecond := startTestWorker(t, cfg, second, sink)
	defer stopFirst()
	defer stopSecond()
	seen := make(map[string]bool)
	for range job.workspaces {
		select {
		case batch := <-delivered:
			if seen[batch.TaskID] {
				t.Fatal("duplicate initial task")
			}
			seen[batch.TaskID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("initial tasks did not finish")
		}
	}
	lost := make(map[string]bool)
	for _, task := range cfg.Tasks() {
		if taskOwner(task.ID, first.Members()) == second.Name() {
			lost[task.ID] = true
		}
	}
	if len(lost) == 0 || len(lost) == len(job.workspaces) {
		t.Fatal("test requires both unaffected and reassigned tasks")
	}
	stopSecond()
	second.closeOnce.Do(func() {
		second.cancel()
		second.wg.Wait()
		second.ready.Store(false)
		second.closeErr = second.list.Shutdown()
	})
	waitFor(t, "failed member eviction without Leave", func() bool {
		return slices.Equal(first.Members(), []string{"first"})
	})
	for range len(lost) {
		select {
		case batch := <-delivered:
			if !lost[batch.TaskID] {
				t.Fatal("failure restarted an unaffected task or duplicated a recovered task")
			}
			delete(lost, batch.TaskID)
		case <-time.After(5 * time.Second):
			t.Fatal("surviving instance did not collect the failed instance's tasks")
		}
	}
	select {
	case <-delivered:
		t.Fatal("extra collection after failure takeover")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClusterSecretValidation(t *testing.T) {
	cfg, err := ParseConfig([]byte("cluster: {bind_address: '127.0.0.1:7946'}\n" + clusterTestConfig))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "not-base64", "YWJj"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("SCRAPER_CLUSTER_SECRET_KEY", key)
			if cluster, err := NewCluster(t.Context(), cfg, quietLogger()); err == nil {
				cluster.Close()
				t.Fatal("invalid encryption key accepted")
			}
		})
	}
}

func TestEncryptedClusterAndUniqueDefaultNames(t *testing.T) {
	t.Setenv("SCRAPER_CLUSTER_SECRET_KEY", "MTIzNDU2Nzg5MDEyMzQ1Ng==")
	cfg, err := ParseConfig([]byte(fmt.Sprintf("cluster: {bind_address: '%s'}\n", availableGossipAddress(t)) + clusterTestConfig))
	if err != nil {
		t.Fatal(err)
	}
	first := startTestCluster(t, cfg)
	cfg.Cluster.Join = []string{first.list.LocalNode().Address()}
	cfg.Cluster.BindAddress = availableGossipAddress(t)
	second := startTestCluster(t, cfg)
	if first.Name() == second.Name() {
		t.Fatal("multiple processes on one host must get distinct node names")
	}
	waitFor(t, "encrypted membership", func() bool {
		return second.Ready() && len(first.Members()) == 2 && len(second.Members()) == 2
	})
}
