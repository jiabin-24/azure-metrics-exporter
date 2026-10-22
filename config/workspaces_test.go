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
