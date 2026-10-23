package metrics

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	AzureInsightBaseMetricsResult struct {
		prober *MetricProber
	}
)

func (r *AzureInsightBaseMetricsResult) sendDataPoints(channel chan<- PrometheusMetricResult, labels prometheus.Labels, data []*armmonitor.MetricValue) {
	for _, point := range data {
		if point.TimeStamp == nil || point.TimeStamp.IsZero() {
			r.prober.recordCollectionError(fmt.Errorf("azure metric data point has no source timestamp"))
			r.prober.logger.Error("Azure metric data point has no source timestamp", slog.Any("labels", labels))
			continue
		}
		for _, aggregation := range []struct {
			name  string
			value *float64
		}{
			{"total", point.Total},
			{"minimum", point.Minimum},
			{"maximum", point.Maximum},
			{"average", point.Average},
			{"count", point.Count},
		} {
			if aggregation.value != nil {
				if r.prober.settings.Format == "json" &&
					(math.IsNaN(*aggregation.value) || math.IsInf(*aggregation.value, 0)) {
					err := fmt.Errorf("azure metric has a non-finite %s value", aggregation.name)
					r.prober.recordCollectionError(err)
					r.prober.logger.Error(err.Error())
					continue
				}
				labels["aggregation"] = aggregation.name
				channel <- r.buildMetric(labels, *aggregation.value, *point.TimeStamp)
			}
		}
	}
}

func (r *AzureInsightBaseMetricsResult) metricFailed(errorCode, errorMessage *string) bool {
	if r.prober.settings.Format != "json" || errorCode == nil ||
		*errorCode == "" || strings.EqualFold(*errorCode, "Success") {
		return false
	}
	message := ""
	if errorMessage != nil {
		message = *errorMessage
	}
	err := fmt.Errorf("azure metric query failed (%s): %s", *errorCode, message)
	r.prober.recordCollectionError(err)
	r.prober.logger.Error(err.Error())
	return true
}

func (r *AzureInsightBaseMetricsResult) buildMetric(labels prometheus.Labels, value float64, timestamp time.Time) (metric PrometheusMetricResult) {
	// copy map to ensure we don't keep references
	metricLabels := prometheus.Labels{}
	for labelName, labelValue := range labels {
		metricLabels[labelName] = labelValue
	}

	metric = PrometheusMetricResult{
		Name:      r.prober.settings.MetricTemplate,
		Labels:    metricLabels,
		Value:     value,
		Timestamp: timestamp,
	}

	// fallback if template is empty (should not be)
	if r.prober.settings.MetricTemplate == "" {
		metric.Name = r.prober.settings.Name
	}

	resourceType := r.prober.settings.ResourceType
	// MetricNamespace is more descriptive than type
	if r.prober.settings.MetricNamespace != "" {
		resourceType = r.prober.settings.MetricNamespace
	}

	// set help
	metric.Help = r.prober.settings.HelpTemplate
	if metricNamePlaceholders.MatchString(metric.Help) {
		metric.Help = metricNamePlaceholders.ReplaceAllStringFunc(
			metric.Help,
			func(fieldName string) string {
				fieldName = strings.Trim(fieldName, "{}")
				switch fieldName {
				case "name":
					return r.prober.settings.Name
				case "type":
					return resourceType
				default:
					if fieldValue, exists := metric.Labels[fieldName]; exists {
						return fieldValue
					}
				}
				return ""
			},
		)
	}

	if metricNamePlaceholders.MatchString(metric.Name) {
		metric.Name = metricNamePlaceholders.ReplaceAllStringFunc(
			metric.Name,
			func(fieldName string) string {
				fieldName = strings.Trim(fieldName, "{}")
				switch fieldName {
				case "name":
					return r.prober.settings.Name
				case "type":
					return resourceType
				default:
					if fieldValue, exists := metric.Labels[fieldName]; exists {
						// remove label, when we add it to metric name
						delete(metric.Labels, fieldName)
						return fieldValue
					}
				}
				return ""
			},
		)
	}

	// sanitize metric name
	metric.Name = metricNameReplacer.Replace(metric.Name)
	metric.Name = strings.ToLower(metric.Name)
	metric.Name = metricNameNotAllowedChars.ReplaceAllString(metric.Name, "")

	return
}
