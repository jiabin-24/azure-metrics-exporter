package main

import (
	"context"
	"io"
	"log/slog"
	"net"
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
	for _, job := range cfg.Jobs {
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
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "metrics-scraper.yaml"))
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
	if _, err := scraper.ParseConfig([]byte(configMap.Data["scraper.yaml"])); err != nil {
		t.Fatalf("ConfigMap contains invalid scrape config: %v", err)
	}
	var deployment struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Replicas int `yaml:"replicas"`
			Strategy struct {
				Type string `yaml:"type"`
			} `yaml:"strategy"`
			Template struct {
				Spec struct {
					Containers []struct {
						Command      []string `yaml:"command"`
						Args         []string `yaml:"args"`
						VolumeMounts []struct {
							Name      string `yaml:"name"`
							MountPath string `yaml:"mountPath"`
							ReadOnly  bool   `yaml:"readOnly"`
						} `yaml:"volumeMounts"`
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
	if deployment.Kind != "Deployment" || deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != "Recreate" || len(containers) != 1 ||
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
	var extraDocument interface{}
	if err := decoder.Decode(&extraDocument); err != io.EOF {
		t.Fatalf("expected only a ConfigMap and Deployment, got another YAML document or error: %v", err)
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
