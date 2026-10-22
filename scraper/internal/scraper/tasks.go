package scraper

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
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

func (c *Config) Tasks() []Task {
	var tasks []Task
	for _, job := range c.Jobs {
		workspaces := job.Workspaces
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
