package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/webdevops/azure-metrics-exporter/config"
)

const (
	azurePrometheusTokenScope = "https://prometheus.monitor.azure.com/.default"
	promQLResultMetricName    = "azure_promql_query_result"
	promQLResultHelp          = "Result of a PromQL query against Azure Monitor workspace."
	maxPromQLResponseSize     = 16 << 20
)

var (
	prometheusMetricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	prometheusLabelNamePattern  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type azurePrometheusResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

type azurePrometheusVectorSample struct {
	Metric map[string]string   `json:"metric"`
	Value  []json.RawMessage   `json:"value"`
	Values [][]json.RawMessage `json:"values"`
}

type promQLSample struct {
	name   string
	labels map[string]string
	value  float64
}

type promQLQueryError struct {
	message string
}

func (e promQLQueryError) Error() string {
	return e.message
}

func probeMetricsPromQLHandler(w http.ResponseWriter, r *http.Request) {
	contextLogger := buildContextLoggerFromRequest(r)
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	params := r.URL.Query()
	query := params.Get("query")
	if strings.TrimSpace(query) == "" {
		http.Error(w, `required query parameter "query" is missing`, http.StatusBadRequest)
		return
	}

	endpoint := Opts.Azure.Prometheus.QueryEndpoint
	if endpoint == "" {
		http.Error(w, "Azure Monitor workspace Prometheus query endpoint is not configured", http.StatusServiceUnavailable)
		return
	}
	if AzureClient == nil {
		http.Error(w, "Azure authentication is not configured", http.StatusServiceUnavailable)
		return
	}

	timeoutSeconds, err := getPrometheusTimeout(r, config.ProbeMetricsPromQLTimeoutDefault)
	if err != nil {
		contextLogger.Warn(err.Error())
		http.Error(w, fmt.Sprintf("failed to parse timeout from Prometheus header: %s", err), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()

	response, statusCode, err := queryAzurePrometheus(ctx, endpoint, params, AzureClient.GetCred(), http.DefaultClient)
	if err != nil {
		contextLogger.With(slog.Any("error", err)).Warn("Azure Monitor PromQL query failed")
		http.Error(w, err.Error(), statusCode)
		return
	}

	metrics, err := formatPromQLResponse(response)
	if err != nil {
		contextLogger.With(slog.Any("error", err)).Warn("unable to format Azure Monitor PromQL response")
		var queryErr promQLQueryError
		if errors.As(err, &queryErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := io.WriteString(w, metrics); err != nil {
		contextLogger.With(slog.Any("error", err)).Error("unable to write PromQL metrics response")
	}
}

func queryAzurePrometheus(
	ctx context.Context,
	endpoint string,
	params url.Values,
	credential azcore.TokenCredential,
	client *http.Client,
) ([]byte, int, error) {
	queryURL, err := azurePrometheusQueryURL(endpoint, params)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{azurePrometheusTokenScope}})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, http.StatusGatewayTimeout, fmt.Errorf("timed out acquiring Azure Monitor Prometheus token: %w", err)
		}
		return nil, http.StatusBadGateway, fmt.Errorf("failed to acquire Azure Monitor Prometheus token: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("failed to create Azure Monitor PromQL request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Accept", "application/json")

	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, http.StatusGatewayTimeout, fmt.Errorf("Azure Monitor PromQL request timed out: %w", err)
		}
		return nil, http.StatusBadGateway, fmt.Errorf("Azure Monitor PromQL request failed: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxPromQLResponseSize+1))
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("failed to read Azure Monitor PromQL response: %w", err)
	}
	if len(body) > maxPromQLResponseSize {
		return nil, http.StatusBadGateway, fmt.Errorf("Azure Monitor PromQL response exceeds the %d byte limit", maxPromQLResponseSize)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnprocessableEntity {
			return nil, http.StatusBadRequest, fmt.Errorf("Azure Monitor rejected the PromQL query: %s", strings.TrimSpace(string(body)))
		}
		return nil, http.StatusBadGateway, fmt.Errorf("Azure Monitor PromQL request returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	return body, http.StatusOK, nil
}

func azurePrometheusQueryURL(endpoint string, requestParams url.Values) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid Azure Monitor workspace Prometheus query endpoint: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("Azure Monitor workspace Prometheus query endpoint must be an HTTPS origin URL")
	}

	parsed.Path = "/api/v1/query"
	params := url.Values{}
	params.Set("query", requestParams.Get("query"))
	rangeQuery := requestParams.Has("start") || requestParams.Has("end") || requestParams.Has("step")
	if rangeQuery {
		for _, name := range []string{"start", "end", "step"} {
			if strings.TrimSpace(requestParams.Get(name)) == "" {
				return "", fmt.Errorf("range queries require non-empty start, end and step parameters")
			}
			params.Set(name, requestParams.Get(name))
		}
		parsed.Path = "/api/v1/query_range"
	}
	parsed.RawQuery = params.Encode()
	return parsed.String(), nil
}

func formatPromQLResponse(body []byte) (string, error) {
	var response azurePrometheusResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("invalid Azure Monitor PromQL response: %w", err)
	}
	if response.Status != "success" {
		if response.Error == "" {
			response.Error = "Azure Monitor returned an unsuccessful PromQL response"
		}
		if response.ErrorType != "" {
			return "", promQLQueryError{message: fmt.Sprintf("%s: %s", response.ErrorType, response.Error)}
		}
		return "", promQLQueryError{message: response.Error}
	}

	var samples []promQLSample
	switch response.Data.ResultType {
	case "vector", "matrix":
		var result []azurePrometheusVectorSample
		if err := json.Unmarshal(response.Data.Result, &result); err != nil {
			return "", fmt.Errorf("invalid PromQL %s result: %w", response.Data.ResultType, err)
		}
		for _, vectorSample := range result {
			point := vectorSample.Value
			if response.Data.ResultType == "matrix" {
				if len(vectorSample.Values) == 0 {
					return "", errors.New("PromQL matrix series does not contain float samples")
				}
				point = vectorSample.Values[len(vectorSample.Values)-1]
			}
			if len(point) != 2 {
				return "", fmt.Errorf("PromQL %s result does not contain a timestamp and value", response.Data.ResultType)
			}
			value, err := parsePromQLValue(point[1])
			if err != nil {
				return "", err
			}

			name := vectorSample.Metric["__name__"]
			if name == "" {
				name = promQLResultMetricName
			}
			labels := make(map[string]string, len(vectorSample.Metric))
			for labelName, labelValue := range vectorSample.Metric {
				if labelName != "__name__" {
					labels[labelName] = labelValue
				}
			}
			samples = append(samples, promQLSample{name: name, labels: labels, value: value})
		}
	case "scalar":
		var result []json.RawMessage
		if err := json.Unmarshal(response.Data.Result, &result); err != nil {
			return "", fmt.Errorf("invalid PromQL scalar result: %w", err)
		}
		if len(result) != 2 {
			return "", errors.New("PromQL scalar result does not contain a timestamp and value")
		}
		value, err := parsePromQLValue(result[1])
		if err != nil {
			return "", err
		}
		samples = append(samples, promQLSample{name: promQLResultMetricName, labels: map[string]string{}, value: value})
	default:
		return "", fmt.Errorf("unsupported PromQL result type %q; vector, scalar and matrix results are supported", response.Data.ResultType)
	}

	return renderPromQLSamples(samples)
}

func parsePromQLValue(rawValue json.RawMessage) (float64, error) {
	var valueString string
	if err := json.Unmarshal(rawValue, &valueString); err != nil {
		return 0, fmt.Errorf("invalid PromQL sample value: %w", err)
	}
	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid PromQL sample value %q: %w", valueString, err)
	}
	return value, nil
}

func renderPromQLSamples(samples []promQLSample) (string, error) {
	groupedSamples := make(map[string][]promQLSample)
	for _, sample := range samples {
		if !prometheusMetricNamePattern.MatchString(sample.name) {
			return "", fmt.Errorf("PromQL result contains invalid Prometheus metric name %q", sample.name)
		}
		for labelName := range sample.labels {
			if !prometheusLabelNamePattern.MatchString(labelName) {
				return "", fmt.Errorf("PromQL result contains invalid Prometheus label name %q", labelName)
			}
		}
		groupedSamples[sample.name] = append(groupedSamples[sample.name], sample)
	}

	names := make([]string, 0, len(groupedSamples))
	for name := range groupedSamples {
		names = append(names, name)
	}
	sort.Strings(names)

	var output strings.Builder
	for _, name := range names {
		fmt.Fprintf(&output, "# HELP %s %s\n# TYPE %s gauge\n", name, promQLResultHelp, name)
		for _, sample := range groupedSamples[name] {
			labelNames := make([]string, 0, len(sample.labels))
			for labelName := range sample.labels {
				labelNames = append(labelNames, labelName)
			}
			sort.Strings(labelNames)

			output.WriteString(name)
			if len(labelNames) > 0 {
				output.WriteByte('{')
				for index, labelName := range labelNames {
					if index > 0 {
						output.WriteByte(',')
					}
					fmt.Fprintf(&output, `%s="%s"`, labelName, escapePrometheusLabelValue(sample.labels[labelName]))
				}
				output.WriteByte('}')
			}
			fmt.Fprintf(&output, " %s\n", strconv.FormatFloat(sample.value, 'g', -1, 64))
		}
	}
	return output.String(), nil
}

func escapePrometheusLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(value)
}
