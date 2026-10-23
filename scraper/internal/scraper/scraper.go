package scraper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

const MaxBodyBytes = 16 << 20

type Batch struct {
	Job         string
	Workspace   string
	TaskID      string
	Target      string
	Labels      map[string]string
	ScrapedAt   time.Time
	ContentType string
	Metrics     []byte
	Families    int
	Window      *metricdata.Window
	Query       string
	Interval    time.Duration
	MetricsPath string
	Params      map[string][]string
}

type Sink func(context.Context, Batch) error

type Scraper struct {
	Config  *Config
	Cluster *Cluster
	Client  *http.Client
	Logger  *slog.Logger
	Sink    Sink
}

func New(cfg *Config, logger *slog.Logger, sink Sink) *Scraper {
	return &Scraper{
		Config: cfg,
		Client: &http.Client{
			// A redirect usually indicates a bad endpoint, not a metrics response.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Logger: logger,
		Sink:   sink,
	}
}

func (s *Scraper) Run(ctx context.Context) {
	defer s.Client.CloseIdleConnections()
	tasks := s.Config.Tasks()
	if s.Cluster != nil {
		s.runClusterTasks(ctx, tasks)
		return
	}
	var workers sync.WaitGroup
	s.Logger.Info("standalone tasks assigned", "tasks", len(tasks))
	for _, task := range tasks {
		workers.Go(func() {
			s.runTarget(ctx, task.Job, task.Target, task.Labels)
		})
	}
	workers.Wait()
}

func (s *Scraper) runClusterTasks(ctx context.Context, tasks []Task) {
	type runner struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	running := make(map[string]runner)
	defer func() {
		for _, task := range running {
			task.cancel()
		}
		for _, task := range running {
			<-task.done
		}
	}()
	reconcile := func() {
		members := s.Cluster.Members()
		wanted := make(map[string]Task)
		if s.Cluster.Ready() {
			for _, task := range tasks {
				if taskOwner(task.ID, members) == s.Cluster.Name() {
					wanted[task.ID] = task
				}
			}
		}
		for id, task := range running {
			if _, keep := wanted[id]; !keep {
				task.cancel()
			}
		}
		for id, task := range running {
			if _, keep := wanted[id]; !keep {
				<-task.done
				delete(running, id)
			}
		}
		for id, task := range wanted {
			if _, exists := running[id]; exists {
				continue
			}
			taskCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			running[id] = runner{cancel, done}
			go func() {
				defer close(done)
				s.runTarget(taskCtx, task.Job, task.Target, task.Labels)
			}()
		}
		s.Logger.Info("cluster tasks assigned", "node", s.Cluster.Name(),
			"members", members, "ready", s.Cluster.Ready(), "tasks", len(running))
	}
	reconcile()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.Cluster.Changes():
			reconcile()
		}
	}
}

func (s *Scraper) runTarget(ctx context.Context, job Job, target string, labels map[string]string) {
	ticker := time.NewTicker(time.Duration(job.Interval))
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		err := s.Scrape(ctx, job, target, labels)
		if err != nil && ctx.Err() == nil {
			s.Logger.Error("scrape failed", "job", job.Name, "target", target,
				"workspace", workspaceFor(job), "duration", time.Since(start), "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scraper) Scrape(ctx context.Context, job Job, target string, labels map[string]string) error {
	job.Params = job.paramsAt(time.Now())
	deliveryCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, time.Duration(job.Timeout))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, job.targetURL(target), nil)
	if err != nil {
		return fmt.Errorf("create scrape request: %w", err)
	}
	expectJSON := len(job.Params["format"]) == 1 && job.Params["format"][0] == "json"
	if expectJSON {
		req.Header.Set("Accept", "application/json")
	} else {
		req.Header.Set("Accept", `text/plain; version=0.0.4; charset=utf-8`)
	}
	req.Header.Set("User-Agent", "azure-metrics-scraper")
	resp, err := s.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request metrics: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metrics endpoint returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read metrics: %w", err)
	}
	if len(body) > MaxBodyBytes {
		return fmt.Errorf("metrics response exceeds %d bytes", MaxBodyBytes)
	}
	batch := Batch{
		Job: job.Name, Target: target, Labels: labels, ScrapedAt: time.Now().UTC(),
		ContentType: resp.Header.Get("Content-Type"), Metrics: body,
		Interval:    time.Duration(job.Interval),
		MetricsPath: job.MetricsPath, Params: job.Params,
		Workspace: workspaceFor(job), TaskID: taskID(job.Name, workspaceFor(job), target),
	}
	if len(job.Params["query"]) == 1 {
		batch.Query = job.Params["query"][0]
	}
	mediaType := ""
	if batch.ContentType != "" {
		mediaType, _, err = mime.ParseMediaType(batch.ContentType)
		if err != nil {
			return fmt.Errorf("invalid metrics Content-Type: %w", err)
		}
	}
	if expectJSON && mediaType != "application/json" {
		return fmt.Errorf("expected application/json for format=json, received %q", batch.ContentType)
	}
	logger := s.Logger.With("workspace", batch.Workspace, "task_id", batch.TaskID)
	if job.RollingRange != nil {
		logger = logger.With("query_start", job.Params["start"][0],
			"query_end", job.Params["end"][0], "query_step", job.Params["step"][0])
	}
	if mediaType == "application/json" {
		batch.Window, err = metricdata.Decode(body)
		if err != nil {
			return err
		}
		if s.Config.EventHub != nil && batch.Window.Source != eventHubSource(job.MetricsPath) {
			return fmt.Errorf("metrics window source %q does not match endpoint %q", batch.Window.Source, job.MetricsPath)
		}
		if job.MetricName != "" {
			if batch.Window.Source != metricdata.PrometheusSource {
				return fmt.Errorf("metric_name requires a PromQL metrics window")
			}
			for i := range batch.Window.Metrics {
				batch.Window.Metrics[i].Name = job.MetricName
			}
			body, err = json.Marshal(batch.Window)
			if err != nil {
				return fmt.Errorf("encode renamed metrics window: %w", err)
			}
			batch.Metrics = body
		}
		names := make(map[string]bool)
		samples := 0
		for _, series := range batch.Window.Metrics {
			names[series.Name] = true
			samples += len(series.Samples)
		}
		batch.Families = len(names)
		logger = logger.With("series", len(batch.Window.Metrics), "samples", samples)
	} else {
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("invalid Prometheus text response: %w", err)
		}
		batch.Families = len(families)
	}
	if err := s.Sink(deliveryCtx, batch); err != nil {
		return fmt.Errorf("deliver scraped metrics: %w", err)
	}
	logger.Info("scrape completed", "job", job.Name, "target", target,
		"metric_families", batch.Families, "bytes", len(body))
	return nil
}

func workspaceFor(job Job) string {
	if values := job.Params["workspace"]; len(values) == 1 {
		return values[0]
	}
	return ""
}

func DiscardSink(logger *slog.Logger) Sink {
	return func(_ context.Context, batch Batch) error {
		logger.Info("Event Hub not configured; discarding batch",
			"job", batch.Job, "target", batch.Target, "workspace", batch.Workspace,
			"task_id", batch.TaskID, "bytes", len(batch.Metrics))
		return nil
	}
}
