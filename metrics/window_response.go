package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

func (l *MetricList) window() metricdata.Window {
	window := metricdata.Window{
		SchemaVersion: metricdata.SchemaVersion,
		Source:        metricdata.Source,
		Metrics:       make([]metricdata.Series, 0),
	}
	names := l.GetMetricNames()
	sort.Strings(names)
	for _, name := range names {
		groups := make(map[string]*metricdata.Series)
		for _, row := range l.GetMetricList(name) {
			labels := make(map[string]string, len(row.Labels))
			for key, value := range row.Labels {
				labels[key] = value
			}
			// String maps always marshal successfully; encoding gives a stable, unambiguous grouping key.
			key, _ := json.Marshal(labels)
			group := groups[string(key)]
			if group == nil {
				group = &metricdata.Series{Name: name, Labels: labels, Samples: make([]metricdata.Sample, 0)}
				groups[string(key)] = group
			}
			group.Samples = append(group.Samples, metricdata.Sample{Timestamp: row.Timestamp, Value: row.Value})
		}
		keys := make([]string, 0, len(groups))
		for key := range groups {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			group := groups[key]
			sort.SliceStable(group.Samples, func(i, j int) bool {
				return group.Samples[i].Timestamp.Before(group.Samples[j].Timestamp)
			})
			window.Metrics = append(window.Metrics, *group)
		}
	}
	return window
}

func (p *MetricProber) recordCollectionError(err error) {
	p.collectionErrorMu.Lock()
	defer p.collectionErrorMu.Unlock()
	if p.collectionErr == nil {
		p.collectionErr = err
	}
}

func (p *MetricProber) collectionError() error {
	if err := p.ctx.Err(); err != nil {
		return err
	}
	p.collectionErrorMu.Lock()
	defer p.collectionErrorMu.Unlock()
	return p.collectionErr
}

func (p *MetricProber) WriteResponse(w http.ResponseWriter, r *http.Request) int {
	if p.settings.Format != "json" {
		promhttp.HandlerFor(p.prometheus.registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
		return http.StatusOK
	}
	if err := p.collectionError(); err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			status = http.StatusGatewayTimeout
		}
		p.logger.Error("unable to return complete platform metrics window", "error", err)
		http.Error(w, "unable to return complete platform metrics window: "+err.Error(), status)
		return status
	}
	body, err := json.Marshal(p.metricList.window())
	if err != nil {
		p.logger.Error("unable to encode platform metrics window", "error", err)
		http.Error(w, fmt.Sprintf("unable to encode platform metrics window: %s", err), http.StatusBadGateway)
		return http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := w.Write(append(body, '\n')); err != nil {
		p.logger.Error("unable to write platform metrics window", "error", err)
	}
	return http.StatusOK
}
