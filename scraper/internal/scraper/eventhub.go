package scraper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/webdevops/azure-metrics-exporter/config"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
)

const eventBodyLimit = 200 * 1024

type exportMetric struct {
	Name    string              `json:"name"`
	PromQL  string              `json:"promql,omitempty"`
	Labels  map[string]string   `json:"labels"`
	Samples []metricdata.Sample `json:"samples"`
}

type exportWindow struct {
	SchemaVersion  string         `json:"schemaVersion"`
	Source         string         `json:"source"`
	Cluster        string         `json:"cluster,omitempty"`
	ResourceID     string         `json:"resourceId,omitempty"`
	Query          *exportQuery   `json:"query,omitempty"`
	Region         string         `json:"region"`
	WindowStart    time.Time      `json:"windowStart"`
	WindowEnd      time.Time      `json:"windowEnd"`
	ScrapeInterval string         `json:"scrapeInterval"`
	Metrics        []exportMetric `json:"metrics"`
	partitionKey   string
}

type exportQuery struct {
	Path   string              `json:"path"`
	Params map[string][]string `json:"params"`
}

type eventSender interface {
	Send(context.Context, []byte, string) error
}

type eventHubSender struct {
	client *azeventhubs.ProducerClient
}

func (s eventHubSender) Send(ctx context.Context, body []byte, partitionKey string) error {
	batch, err := s.client.NewEventDataBatch(ctx, &azeventhubs.EventDataBatchOptions{
		PartitionKey: &partitionKey, MaxBytes: 256 * 1024,
	})
	if err != nil {
		return fmt.Errorf("create Event Hub batch: %w", err)
	}
	contentType := "application/json"
	if err := batch.AddEventData(&azeventhubs.EventData{Body: body, ContentType: &contentType}, nil); err != nil {
		return fmt.Errorf("add Event Hub event: %w", err)
	}
	return s.client.SendEventDataBatch(ctx, batch, nil)
}

func validateConnectionString(connection string, cfg EventHubConfig) error {
	fields := make(map[string]string)
	for _, part := range strings.Split(connection, ";") {
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || value == "" || fields[key] != "" {
			return fmt.Errorf("invalid Event Hub SAS connection string")
		}
		switch key {
		case "Endpoint", "SharedAccessKeyName", "SharedAccessKey", "EntityPath":
			fields[key] = value
		default:
			return fmt.Errorf("unsupported Event Hub SAS connection string field")
		}
	}
	endpoint, err := url.Parse(fields["Endpoint"])
	if err != nil || endpoint.Scheme != "sb" || !strings.EqualFold(endpoint.Host, cfg.Namespace) ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		fields["EntityPath"] != cfg.Name || fields["SharedAccessKeyName"] == "" || fields["SharedAccessKey"] == "" {
		return fmt.Errorf("AZURE_EVENTHUB_CONNECTION_STRING must contain SAS key credentials and match the configured namespace and EntityPath")
	}
	return nil
}

func NewEventHubSink(cfg EventHubConfig, logger *slog.Logger) (Sink, func(context.Context) error, error) {
	connection := os.Getenv("AZURE_EVENTHUB_CONNECTION_STRING")
	if err := validateConnectionString(connection, cfg); err != nil {
		return nil, nil, err
	}
	client, err := azeventhubs.NewProducerClientFromConnectionString(connection, "", nil)
	if err != nil {
		// Do not include SDK constructor errors that might contain credential input.
		return nil, nil, fmt.Errorf("create Event Hub SAS producer: invalid connection configuration")
	}
	return eventHubSink(eventHubSender{client}, logger), client.Close, nil
}

func eventHubSink(sender eventSender, logger *slog.Logger) Sink {
	return func(ctx context.Context, batch Batch) error {
		windows, err := exportWindows(batch)
		if err != nil {
			return err
		}
		type event struct {
			body         []byte
			partitionKey string
		}
		var events []event
		for _, window := range windows {
			if window.Region == "" {
				logger.Warn("region/location label missing; exporting empty region",
					"job", batch.Job, "cluster", window.Cluster, "resource_id", window.ResourceID)
			}
			if window.Source == metricdata.Source && window.ResourceID == "" {
				logger.Warn("resourceID label missing; exporting without resourceId",
					"job", batch.Job, "target", batch.Target)
			}
			bodies, err := splitWindow(window, eventBodyLimit)
			if err != nil {
				return err
			}
			for _, body := range bodies {
				events = append(events, event{body, window.partitionKey})
			}
		}
		if len(events) == 0 {
			logger.Info("empty metrics result; no Event Hub events sent", "job", batch.Job, "target", batch.Target)
			return nil
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for i, event := range events {
			if err := sender.Send(ctx, event.body, event.partitionKey); err != nil {
				return fmt.Errorf("Event Hub delivery failed after %d/%d acknowledged events (delivery may be partial): %w", i, len(events), err)
			}
		}
		logger.Info("Event Hub delivery completed", "job", batch.Job, "target", batch.Target, "events", len(events))
		return nil
	}
}

func exportWindows(batch Batch) ([]exportWindow, error) {
	if batch.Window == nil || batch.Interval <= 0 {
		return nil, fmt.Errorf("Event Hub export requires a metrics JSON batch with scrape interval")
	}
	platform := batch.Window.Source == metricdata.Source
	if !platform && (batch.Window.Source != metricdata.PrometheusSource || batch.Query == "") {
		return nil, fmt.Errorf("unsupported Event Hub metrics source or missing PromQL query")
	}
	if platform && (eventHubSource(batch.MetricsPath) != metricdata.Source || batch.Params == nil) {
		return nil, fmt.Errorf("platform Event Hub export requires a platform probe path and query parameters")
	}
	type groupKey struct{ identity, region string }
	groups := make(map[groupKey]*exportWindow)
	var keys []groupKey
	for _, series := range batch.Window.Metrics {
		if len(series.Samples) == 0 {
			continue
		}
		var cluster, resourceID string
		if platform {
			resourceID = series.Labels["resourceID"]
			// A single explicit target is unambiguous even when metric templates remove the resourceID label.
			if resourceID == "" && batch.MetricsPath == config.ProbeMetricsResourceUrl && len(batch.Params["target"]) == 1 {
				resourceID = batch.Params["target"][0]
			}
		} else {
			cluster = series.Labels["cluster"]
			if cluster == "" {
				cluster = batch.Labels["cluster"]
			}
			if cluster == "" {
				return nil, fmt.Errorf("metric %q has no cluster label or static cluster metadata", series.Name)
			}
		}
		region := series.Labels["region"]
		if region == "" {
			region = series.Labels["location"]
		}
		if region == "" {
			region = batch.Labels["region"]
		}
		if region == "" {
			region = batch.Labels["location"]
		}
		identity := cluster
		if platform {
			identity = strings.ToLower(resourceID)
		}
		key := groupKey{identity, region}
		window := groups[key]
		if window == nil {
			window = &exportWindow{
				SchemaVersion: metricdata.SchemaVersion, Source: "aks-controlplane",
				Cluster: cluster, Region: region, ScrapeInterval: batch.Interval.String(),
				Metrics:      make([]exportMetric, 0),
				partitionKey: cluster,
			}
			if platform {
				window.Source, window.ResourceID = metricdata.Source, resourceID
				window.Query = &exportQuery{Path: batch.MetricsPath, Params: batch.Params}
				partitionIdentity := identity
				if partitionIdentity == "" {
					partitionIdentity = batch.Job + "\x00" + batch.Target + "\x00" +
						batch.MetricsPath + "\x00" + url.Values(batch.Params).Encode()
				}
				// ARM resource IDs can exceed the Event Hubs partition key length limit.
				window.partitionKey = fmt.Sprintf("%x", sha256.Sum256([]byte(partitionIdentity)))
			}
			groups[key] = window
			keys = append(keys, key)
		}
		for _, point := range series.Samples {
			if window.WindowStart.IsZero() || point.Timestamp.Before(window.WindowStart) {
				window.WindowStart = point.Timestamp.UTC()
			}
			if window.WindowEnd.IsZero() || point.Timestamp.After(window.WindowEnd) {
				window.WindowEnd = point.Timestamp.UTC()
			}
		}
		labels := make(map[string]string, len(series.Labels))
		for name, value := range series.Labels {
			labels[name] = value
		}
		window.Metrics = append(window.Metrics, exportMetric{
			Name: series.Name, Labels: labels, Samples: series.Samples,
		})
		if !platform {
			window.Metrics[len(window.Metrics)-1].PromQL = batch.Query
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].identity != keys[j].identity {
			return keys[i].identity < keys[j].identity
		}
		return keys[i].region < keys[j].region
	})
	windows := make([]exportWindow, 0, len(keys))
	for _, key := range keys {
		windows = append(windows, *groups[key])
	}
	return windows, nil
}

func splitWindow(window exportWindow, limit int) ([][]byte, error) {
	body, err := json.Marshal(window)
	if err != nil {
		return nil, fmt.Errorf("encode metrics event: %w", err)
	}
	if len(body) <= limit {
		return [][]byte{body}, nil
	}
	left, right := window, window
	if len(window.Metrics) > 1 {
		mid := len(window.Metrics) / 2
		left.Metrics, right.Metrics = window.Metrics[:mid], window.Metrics[mid:]
	} else if len(window.Metrics) == 1 && len(window.Metrics[0].Samples) > 1 {
		first, second := window.Metrics[0], window.Metrics[0]
		mid := len(first.Samples) / 2
		first.Samples, second.Samples = first.Samples[:mid], second.Samples[mid:]
		left.Metrics, right.Metrics = []exportMetric{first}, []exportMetric{second}
	} else {
		return nil, fmt.Errorf("single metric sample exceeds Event Hub event body limit of %d bytes", limit)
	}
	first, err := splitWindow(left, limit)
	if err != nil {
		return nil, err
	}
	second, err := splitWindow(right, limit)
	if err != nil {
		return nil, err
	}
	return append(first, second...), nil
}
