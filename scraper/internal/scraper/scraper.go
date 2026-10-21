package scraper

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const MaxBodyBytes = 16 << 20

type Batch struct {
	Job         string
	Target      string
	Labels      map[string]string
	ScrapedAt   time.Time
	ContentType string
	Metrics     []byte
	Families    int
}

type Sink func(context.Context, Batch) error

type Scraper struct {
	Config *Config
	Client *http.Client
	Logger *slog.Logger
	Sink   Sink
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
	var workers sync.WaitGroup
	for _, job := range s.Config.Jobs {
		for _, group := range job.StaticConfigs {
			for _, target := range group.Targets {
				workers.Go(func() {
					s.runTarget(ctx, job, target, group.Labels)
				})
			}
		}
	}
	workers.Wait()
	s.Client.CloseIdleConnections()
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
				"duration", time.Since(start), "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scraper) Scrape(ctx context.Context, job Job, target string, labels map[string]string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(job.Timeout))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, job.targetURL(target), nil)
	if err != nil {
		return fmt.Errorf("create scrape request: %w", err)
	}
	req.Header.Set("Accept", `text/plain; version=0.0.4; charset=utf-8`)
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
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("invalid Prometheus text response: %w", err)
	}
	batch := Batch{
		Job: job.Name, Target: target, Labels: labels, ScrapedAt: time.Now().UTC(),
		ContentType: resp.Header.Get("Content-Type"), Metrics: body, Families: len(families),
	}
	if err := s.Sink(ctx, batch); err != nil {
		return fmt.Errorf("deliver scraped metrics: %w", err)
	}
	s.Logger.Info("scrape completed", "job", job.Name, "target", target,
		"metric_families", len(families), "bytes", len(body))
	return nil
}

func PendingEventHubSink(logger *slog.Logger) Sink {
	return func(_ context.Context, batch Batch) error {
		// TODO: Send the batch to Azure Event Hubs with bounded retries and backpressure.
		logger.Info("Event Hub delivery not implemented; discarding batch",
			"job", batch.Job, "target", batch.Target, "bytes", len(batch.Metrics))
		return nil
	}
}
