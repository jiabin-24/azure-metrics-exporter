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

func TestDeploymentConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "metrics-scraper.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	var configMap struct {
		Kind string            `yaml:"kind"`
		Data map[string]string `yaml:"data"`
	}
	if err := decoder.Decode(&configMap); err != nil {
		t.Fatal(err)
	}
	if configMap.Kind != "ConfigMap" {
		t.Fatalf("unexpected config kind: %s", configMap.Kind)
	}
	if _, err := scraper.ParseConfig([]byte(configMap.Data["scraper.yaml"])); err != nil {
		t.Fatalf("deployment contains invalid scrape config: %v", err)
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
						Command []string `yaml:"command"`
						Args    []string `yaml:"args"`
					} `yaml:"containers"`
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
		len(containers[0].Args) != 2 {
		t.Fatalf("unexpected deployment: %+v", deployment)
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
