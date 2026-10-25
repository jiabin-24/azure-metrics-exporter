package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/webdevops/azure-metrics-exporter/scraper/internal/scraper"
	"go.yaml.in/yaml/v2"
)

func TestLocalConfig(t *testing.T) {
	cfg, err := scraper.LoadConfig(filepath.Join("..", "..", "scraper.yaml"))
	if err != nil {
		t.Fatalf("project contains invalid scrape config: %v", err)
	}
	if len(cfg.Jobs) != 18 {
		t.Fatalf("local config should contain 18 metric jobs, got %d", len(cfg.Jobs))
	}
	for _, job := range cfg.Jobs {
		if job.MetricName != job.Name {
			t.Fatalf("local job %q should explicitly export its business metric name, got %q", job.Name, job.MetricName)
		}
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				if target != "127.0.0.1:8080" {
					t.Fatalf("local job %q should target the local exporter, got %q", job.Name, target)
				}
			}
		}
	}
}

func TestDeploymentConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "metrics-scraper.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	var configMap struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}
	if err := decoder.Decode(&configMap); err != nil {
		t.Fatal(err)
	}
	if configMap.Kind != "ConfigMap" || configMap.Metadata.Name != "azure-metrics-scraper" {
		t.Fatalf("unexpected ConfigMap: %+v", configMap)
	}
	config, err := scraper.ParseConfig([]byte(configMap.Data["scraper.yaml"]))
	if err != nil {
		t.Fatalf("ConfigMap contains invalid scrape config: %v", err)
	}
	if config.EventHub == nil {
		t.Fatal("deployment must enable Event Hub delivery")
	}
	for _, job := range config.Jobs {
		if time.Duration(job.Interval) != time.Minute || job.RollingRange == nil ||
			job.MetricsPath != "/probe/metrics/promql" || len(job.Params["format"]) != 1 ||
			job.Params["format"][0] != "json" {
			t.Fatalf("expected scheduled PromQL JSON collection: %+v", job)
		}
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				if target != "azure-metrics:8080" {
					t.Fatalf("target must use the exporter service: %q", target)
				}
			}
		}
	}
	if config.Cluster == nil || len(config.Cluster.Join) != 1 ||
		config.Cluster.Join[0] != "azure-metrics-scraper-gossip:7946" {
		t.Fatal("deployment must bootstrap using the gossip service")
	}
	var deployment struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Replicas int `yaml:"replicas"`
			Strategy struct {
				Type string `yaml:"type"`
			} `yaml:"strategy"`
			Template struct {
				Metadata struct {
					Labels map[string]string `yaml:"labels"`
				} `yaml:"metadata"`
				Spec struct {
					Containers []struct {
						Command      []string `yaml:"command"`
						Args         []string `yaml:"args"`
						VolumeMounts []struct {
							Name      string `yaml:"name"`
							MountPath string `yaml:"mountPath"`
							ReadOnly  bool   `yaml:"readOnly"`
						} `yaml:"volumeMounts"`
						Env []struct {
							Name      string `yaml:"name"`
							Value     string `yaml:"value"`
							ValueFrom struct {
								FieldRef struct {
									FieldPath string `yaml:"fieldPath"`
								} `yaml:"fieldRef"`
							} `yaml:"valueFrom"`
						} `yaml:"env"`
						Ports []struct {
							Name     string `yaml:"name"`
							Port     int    `yaml:"containerPort"`
							Protocol string `yaml:"protocol"`
						} `yaml:"ports"`
					} `yaml:"containers"`
					Volumes []struct {
						Name      string `yaml:"name"`
						ConfigMap struct {
							Name string `yaml:"name"`
						} `yaml:"configMap"`
					} `yaml:"volumes"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := decoder.Decode(&deployment); err != nil {
		t.Fatal(err)
	}
	containers := deployment.Spec.Template.Spec.Containers
	if deployment.Kind != "Deployment" || deployment.Spec.Replicas != 2 ||
		deployment.Spec.Strategy.Type != "RollingUpdate" || len(containers) != 1 ||
		len(containers[0].Command) != 1 || containers[0].Command[0] != "/azure-metrics-scraper" ||
		len(containers[0].Args) != 2 ||
		containers[0].Args[0] != "--config.file=/etc/azure-metrics-scraper/scraper.yaml" ||
		len(containers[0].VolumeMounts) != 1 || len(deployment.Spec.Template.Spec.Volumes) != 1 {
		t.Fatalf("unexpected deployment: %+v", deployment)
	}
	mount := containers[0].VolumeMounts[0]
	volume := deployment.Spec.Template.Spec.Volumes[0]
	if mount.Name != volume.Name || mount.MountPath != "/etc/azure-metrics-scraper" ||
		!mount.ReadOnly || volume.ConfigMap.Name != configMap.Metadata.Name {
		t.Fatalf("deployment does not mount the ConfigMap correctly: mount=%+v volume=%+v", mount, volume)
	}
	foundIP, foundAdvertise := false, false
	for _, env := range containers[0].Env {
		if env.Name == "POD_IP" && env.ValueFrom.FieldRef.FieldPath == "status.podIP" {
			foundIP = true
		}
		if env.Name == "SCRAPER_CLUSTER_ADVERTISE_ADDRESS" && env.Value == "$(POD_IP):7946" {
			foundAdvertise = true
		}
	}
	if !foundIP || !foundAdvertise {
		t.Fatal("gossip must advertise the actual Pod IP")
	}
	containerPorts := make(map[string]string)
	for _, port := range containers[0].Ports {
		if port.Port == 7946 {
			containerPorts[port.Name] = port.Protocol
		}
	}
	if containerPorts["gossip-tcp"] != "TCP" || containerPorts["gossip-udp"] != "UDP" {
		t.Fatal("gossip requires both TCP and UDP")
	}
	var service struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Spec struct {
			ClusterIP       string            `yaml:"clusterIP"`
			PublishNotReady bool              `yaml:"publishNotReadyAddresses"`
			Selector        map[string]string `yaml:"selector"`
			Ports           []struct {
				Port       int    `yaml:"port"`
				TargetPort string `yaml:"targetPort"`
				Protocol   string `yaml:"protocol"`
			} `yaml:"ports"`
		} `yaml:"spec"`
	}
	if err := decoder.Decode(&service); err != nil {
		t.Fatal(err)
	}
	if service.Kind != "Service" || service.Metadata.Name != "azure-metrics-scraper-gossip" ||
		service.Spec.ClusterIP != "None" || !service.Spec.PublishNotReady ||
		len(service.Spec.Selector) == 0 || len(service.Spec.Ports) != 2 {
		t.Fatalf("expected a headless bootstrap service: %+v", service)
	}
	for key, value := range service.Spec.Selector {
		if deployment.Spec.Template.Metadata.Labels[key] != value {
			t.Fatal("headless service does not select scraper Pods")
		}
	}
	for _, port := range service.Spec.Ports {
		if port.Port != 7946 || containerPorts[port.TargetPort] != port.Protocol {
			t.Fatal("headless service does not route gossip protocols to container ports")
		}
	}
	var extraDocument interface{}
	if err := decoder.Decode(&extraDocument); !errors.Is(err, io.EOF) {
		t.Fatalf("expected only a ConfigMap, Deployment and headless Service, got another YAML document or error: %v", err)
	}
}

func availableGossipAddress(t *testing.T) string {
	t.Helper()
	var lastErr error
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

func TestClusterHealthWaitsForInitialJoin(t *testing.T) {
	seed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "scraper.yaml")
	data := fmt.Sprintf("cluster: {bind_address: '%s', join: ['%s']}\nscrape_configs:\n- job_name: test\n  static_configs:\n  - targets: [localhost:1]\n", availableGossipAddress(t), seed.Addr())
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, path, address, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("cluster runtime shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("cluster runtime did not shut down")
		}
	}()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("liveness status: %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("health server did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := client.Get("http://" + address + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unjoined instance must not be ready: %d", response.StatusCode)
	}
}

func TestRunLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(context.Background(), filepath.Join(t.TempDir(), "missing.yaml"), ":0", logger); err == nil {
		t.Fatal("missing config must fail startup")
	}

	configPath := filepath.Join(t.TempDir(), "scraper.yaml")
	if err := os.WriteFile(configPath, []byte("scrape_configs:\n- job_name: test\n  static_configs:\n  - targets: [localhost:1]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := run(context.Background(), configPath, listener.Addr().String(), logger); err == nil {
		t.Fatal("occupied health port must fail startup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, configPath, "127.0.0.1:0", logger) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func TestRunAllWorkspaces(t *testing.T) {
	requests := make(chan string, 4)
	exporter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/probe/metrics/promql/workspaces" {
			fmt.Fprint(w, `{"workspaces":["b","a"]}`)
			return
		}
		if r.URL.Path != "/probe/metrics/promql" {
			t.Errorf("unexpected scrape path: %s", r.URL.Path)
		}
		requests <- r.URL.Query().Get("workspace")
		fmt.Fprint(w, "up 1\n")
	}))
	defer exporter.Close()
	path := filepath.Join(t.TempDir(), "scraper.yaml")
	data := fmt.Sprintf("global: {scrape_interval: 1h, scrape_timeout: 1s}\nscrape_configs:\n- job_name: all\n  metrics_path: /probe/metrics/promql\n  static_configs:\n  - targets: ['%s']\n", strings.TrimPrefix(exporter.URL, "http://"))
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, path, "127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("all-workspace runtime shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("all-workspace runtime did not shut down")
		}
	}()
	seen := make(map[string]bool)
	for range 2 {
		select {
		case workspace := <-requests:
			if (workspace != "a" && workspace != "b") || seen[workspace] {
				t.Fatalf("invalid or duplicate workspace request: %q", workspace)
			}
			seen[workspace] = true
		case <-time.After(3 * time.Second):
			t.Fatal("scraper did not start a task for every workspace")
		}
	}
}

func TestRunEventHubRequiresCredentials(t *testing.T) {
	t.Setenv("AZURE_EVENTHUB_CONNECTION_STRING", "")
	err := run(context.Background(), filepath.Join("..", "..", "scraper.yaml"), "127.0.0.1:0",
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "AZURE_EVENTHUB_CONNECTION_STRING") {
		t.Fatalf("missing Event Hub credentials must fail startup, got %v", err)
	}
}
