package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"

	"go.yaml.in/yaml/v2"
)

var workspaceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func ValidWorkspaceName(name string) bool {
	return workspaceName.MatchString(name)
}

func LoadWorkspaces(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Prometheus workspaces: %w", err)
	}
	var cfg struct {
		Workspaces map[string]struct {
			QueryEndpoint string `yaml:"query_endpoint"`
		} `yaml:"workspaces"`
	}
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse Prometheus workspaces: %w", err)
	}
	if len(cfg.Workspaces) == 0 {
		return nil, fmt.Errorf("workspaces file requires at least one workspace")
	}
	endpoints := make(map[string]string, len(cfg.Workspaces))
	for name, workspace := range cfg.Workspaces {
		if !ValidWorkspaceName(name) {
			return nil, fmt.Errorf("invalid workspace alias %q", name)
		}
		u, err := url.Parse(workspace.QueryEndpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
			(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return nil, fmt.Errorf("workspace %q requires an HTTPS origin query_endpoint", name)
		}
		endpoints[name] = workspace.QueryEndpoint
	}
	return endpoints, nil
}
