package scraper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/webdevops/azure-metrics-exporter/config"
)

type Task struct {
	ID     string
	Job    Job
	Target string
	Labels map[string]string
}

func taskID(job, workspace, target string) string {
	data, _ := json.Marshal([]string{job, workspace, target})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (s *Scraper) ResolveWorkspaces(ctx context.Context) error {
	var jobs []Job
	cache := make(map[string][]string)
	for _, job := range s.Config.Jobs {
		if job.MetricsPath != config.ProbeMetricsPromQLUrl || job.workspaces != nil {
			jobs = append(jobs, job)
			continue
		}
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				address := url.URL{Scheme: job.Scheme, Host: target, Path: config.ProbeMetricsPromQLWorkspacesUrl}
				names, cached := cache[address.String()]
				if !cached {
					var err error
					names, err = s.discoverWorkspaces(ctx, address.String(), time.Duration(job.Timeout))
					if err != nil {
						return fmt.Errorf("job %q: discover workspaces from %s: %w", job.Name, address.String(), err)
					}
					cache[address.String()] = names
				}
				expanded := job
				expanded.workspaces = names
				expanded.StaticConfigs = []StaticConfig{{Targets: []string{target}, Labels: group.Labels}}
				jobs = append(jobs, expanded)
			}
		}
	}
	s.Config.Jobs = jobs
	return nil
}

func (s *Scraper) discoverWorkspaces(ctx context.Context, address string, timeout time.Duration) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, fmt.Errorf("create discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "azure-metrics-scraper")
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request workspace list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("workspace list returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read workspace list: %w", err)
	}
	if len(body) > MaxBodyBytes {
		return nil, fmt.Errorf("workspace list exceeds %d bytes", MaxBodyBytes)
	}
	var result struct {
		Workspaces []string `json:"workspaces"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode workspace list: %w", err)
	}
	if len(result.Workspaces) == 0 {
		return nil, fmt.Errorf("workspace list must not be empty")
	}
	seen := make(map[string]bool, len(result.Workspaces))
	for _, name := range result.Workspaces {
		if !config.ValidWorkspaceName(name) || seen[name] {
			return nil, fmt.Errorf("workspace list contains invalid or duplicate alias %q", name)
		}
		seen[name] = true
	}
	slices.Sort(result.Workspaces)
	return result.Workspaces, nil
}

func (c *Config) Tasks() []Task {
	var tasks []Task
	for _, job := range c.Jobs {
		workspaces := job.workspaces
		if len(workspaces) == 0 {
			workspaces = []string{""}
		}
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				for _, workspace := range workspaces {
					id := taskID(job.Name, workspace, target)
					taskJob := job
					taskJob.Params = maps.Clone(job.Params)
					if workspace != "" {
						if taskJob.Params == nil {
							taskJob.Params = make(map[string][]string)
						}
						taskJob.Params["workspace"] = []string{workspace}
					}
					tasks = append(tasks, Task{ID: id, Job: taskJob, Target: target, Labels: group.Labels})
				}
			}
		}
	}
	return tasks
}
