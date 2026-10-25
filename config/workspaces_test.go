package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jessevdk/go-flags"
)

func TestLoadWorkspaces(t *testing.T) {
	for name, input := range map[string]string{
		"single":    "workspaces:\n  prod:\n    query_endpoint: https://prod.example.com\n",
		"multiple":  "workspaces:\n  prod:\n    query_endpoint: https://prod.example.com\n  test:\n    query_endpoint: https://test.example.com\n",
		"empty":     "workspaces: {}\n",
		"alias":     "workspaces:\n  bad/name:\n    query_endpoint: https://prod.example.com\n",
		"http":      "workspaces:\n  prod:\n    query_endpoint: http://prod.example.com\n",
		"path":      "workspaces:\n  prod:\n    query_endpoint: https://prod.example.com/api/v1/query\n",
		"unknown":   "workspaces:\n  prod:\n    endpoint: https://prod.example.com\n",
		"duplicate": "workspaces:\n  prod:\n    query_endpoint: https://prod.example.com\n  prod:\n    query_endpoint: https://other.example.com\n",
		"query":     "workspaces:\n  prod:\n    query_endpoint: https://prod.example.com?\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workspaces.yaml")
			if err := os.WriteFile(path, []byte(input), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := LoadWorkspaces(path)
			if name != "single" && name != "multiple" {
				if err == nil {
					t.Fatal("invalid configuration was accepted")
				}
				return
			}
			if err != nil || got["prod"] != "https://prod.example.com" ||
				(name == "single" && len(got) != 1) || (name == "multiple" && len(got) != 2) {
				t.Fatalf("workspaces=%v err=%v", got, err)
			}
		})
	}
	if _, err := LoadWorkspaces(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing file was accepted")
	}
}

func TestProjectWorkspaces(t *testing.T) {
	workspaces, err := LoadWorkspaces("prometheus-workspaces.yaml")
	if err != nil {
		t.Fatalf("project contains invalid workspace config: %v", err)
	}
	if len(workspaces) == 0 {
		t.Fatal("project workspace config must not be empty")
	}
}

func TestWorkspaceOptions(t *testing.T) {
	t.Setenv("AZURE_PROMETHEUS_WORKSPACES_FILE", "workspaces.yaml")
	var opts Opts
	parser := flags.NewParser(&opts, flags.None)
	if _, err := parser.ParseArgs(nil); err != nil {
		t.Fatal(err)
	}

	if opts.Azure.Prometheus.WorkspacesFile != "workspaces.yaml" {
		t.Fatal("workspace environment option was not parsed")
	}
	if _, err := parser.ParseArgs([]string{"--azure.prometheus.query-endpoint=https://old.example.com"}); err == nil {
		t.Fatal("legacy endpoint flag was accepted")
	}
}

func TestWorkspaceFileDefaultsAndOverrides(t *testing.T) {
	t.Setenv("AZURE_PROMETHEUS_WORKSPACES_FILE", "")
	if err := os.Unsetenv("AZURE_PROMETHEUS_WORKSPACES_FILE"); err != nil {
		t.Fatal(err)
	}
	envPath := "environment.yaml"
	emptyPath := ""
	defaultPath := "/etc/azure-metrics-exporter/workspaces.yaml"
	for _, test := range []struct {
		name       string
		env        *string
		args       []string
		want       string
		useDefault bool
	}{
		{name: "default", want: "/etc/azure-metrics-exporter/workspaces.yaml", useDefault: true},
		{name: "flag", args: []string{"--azure.prometheus.workspaces-file=custom.yaml"}, want: "custom.yaml"},
		{name: "disabled", args: []string{"--azure.prometheus.workspaces-file="}},
		{name: "explicit default", args: []string{"--azure.prometheus.workspaces-file=/etc/azure-metrics-exporter/workspaces.yaml"}, want: "/etc/azure-metrics-exporter/workspaces.yaml"},
		{name: "environment", env: &envPath, want: envPath},
		{name: "empty environment", env: &emptyPath},
		{name: "explicit default environment", env: &defaultPath, want: defaultPath},
		{name: "flag overrides environment", env: &envPath, args: []string{"--azure.prometheus.workspaces-file=custom.yaml"}, want: "custom.yaml"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.env != nil {
				t.Setenv("AZURE_PROMETHEUS_WORKSPACES_FILE", *test.env)
			}
			var opts Opts
			parser := flags.NewParser(&opts, flags.None)
			if _, err := parser.ParseArgs(test.args); err != nil {
				t.Fatal(err)
			}
			if got := opts.Azure.Prometheus.WorkspacesFile; got != test.want {
				t.Fatalf("workspace file = %q, want %q", got, test.want)
			}
			_, envSet := os.LookupEnv("AZURE_PROMETHEUS_WORKSPACES_FILE")
			if got := parser.FindOptionByLongName("azure.prometheus.workspaces-file").IsSetDefault() && !envSet; got != test.useDefault {
				t.Fatalf("default option = %v, want %v", got, test.useDefault)
			}
		})
	}
}
