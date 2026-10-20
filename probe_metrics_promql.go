package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
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
	azurePrometheusTokenScope = "https://prometheus.monitor.azure.com/.default" // #nosec G101 -- Public OAuth scope URL, not a credential.
	promQLResultMetricName    = "azure_promql_query_result"
	promQLResultHelp          = "Result of a PromQL query against Azure Monitor workspace."
	maxPromQLResponseSize     = 16 << 20
)

var (
	prometheusMetricNamePattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	prometheusLabelNamePattern  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	promQLStandaloneSelector    = regexp.MustCompile(`^\{(?:[^"{}]|"(?:\\.|[^"\\])*")*\}$`)
	promQLSelectorMatcher       = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*("(?:\\.|[^"\\])*")\s*`)
	promQLNamedSelector         = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*\{(?:[^"{}]|"(?:\\.|[^"\\])*")*\}$`)
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
	name            string
	labels          map[string]string
	value           float64
	timestampMillis int64
	rawValue        json.RawMessage
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

	metrics, statusCode, err := collectPromQLMetrics(ctx, endpoint, params, AzureClient.GetCred(), http.DefaultClient)
	if err != nil {
		contextLogger.With(slog.Any("error", err)).Warn("Azure Monitor PromQL query failed")
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, err := io.WriteString(w, metrics); err != nil { // #nosec G705 -- Prometheus text, not HTML; explicit text/plain and nosniff prevent HTML interpretation. HTML escaping would corrupt label values.
		contextLogger.With(slog.Any("error", err)).Error("unable to write PromQL metrics response")
	}
}

func collectPromQLMetrics(
	ctx context.Context,
	endpoint string,
	params url.Values,
	credential azcore.TokenCredential,
	client *http.Client,
) (string, int, error) {
	queryParams := make(url.Values, len(params)+1)
	for name, values := range params {
		queryParams[name] = append([]string(nil), values...)
	}
	if !params.Has("start") && !params.Has("end") && !params.Has("step") && !params.Has("time") {
		// Both requests must evaluate the same instant, even across a new upstream scrape.
		queryParams.Set("time", time.Now().UTC().Format(time.RFC3339Nano))
	}
	body, status, err := queryAzurePrometheus(ctx, endpoint, queryParams, credential, client)
	if err != nil {
		return "", status, err
	}
	samples, err := parsePromQLResponse(body)
	if err != nil {
		var queryErr promQLQueryError
		if errors.As(err, &queryErr) {
			return "", http.StatusBadRequest, err
		}
		return "", http.StatusBadGateway, err
	}

	query := strings.TrimSpace(params.Get("query"))
	isSelector := prometheusMetricNamePattern.MatchString(query) ||
		promQLStandaloneSelector.MatchString(query) || promQLNamedSelector.MatchString(query)
	if isSelector && len(samples) > 0 {
		selectors, err := promQLMetricListSelectors(query)
		if err != nil {
			return "", http.StatusBadRequest, err
		}
		nameLabel := "azure_metrics_exporter_source_name"
		labelNames := make(map[string]bool)
		for _, sample := range samples {
			for name := range sample.labels {
				labelNames[name] = true
			}
		}
		for labelNames[nameLabel] {
			nameLabel += "_"
		}
		sourceQuery, err := promQLSourceTimestampQuery(selectors, samples, nameLabel)
		if err != nil {
			return "", http.StatusBadGateway, err
		}
		queryParams.Set("query", sourceQuery)
		body, status, err = queryAzurePrometheus(ctx, endpoint, queryParams, credential, client)
		if err != nil {
			return "", status, fmt.Errorf("failed to query source sample timestamps: %w", err)
		}
		timestamps, err := parsePromQLResponse(body)
		if err != nil {
			return "", http.StatusBadGateway, fmt.Errorf("invalid source timestamp response: %w", err)
		}
		if err := applyPromQLSourceTimestamps(samples, timestamps, nameLabel); err != nil {
			return "", http.StatusBadGateway, err
		}
	}

	output, err := renderPromQLSamples(samples)
	if err != nil {
		return "", http.StatusBadGateway, err
	}
	return output, http.StatusOK, nil
}

func promQLSourceTimestampQuery(selectors []string, samples []promQLSample, nameLabel string) (string, error) {
	names := make(map[string]bool)
	for _, sample := range samples {
		if !prometheusMetricNamePattern.MatchString(sample.name) {
			return "", errors.New("raw selector response contains an invalid metric name")
		}
		names[sample.name] = true
	}
	sortedNames := make([]string, 0, len(names))
	for name := range names {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	var expressions []string
	add := func(selector, name string) {
		// timestamp() must receive the selector directly: wrapping it in
		// label_replace() first would replace source time with evaluation time.
		expressions = append(expressions, fmt.Sprintf(
			`label_replace(timestamp(%s), %q, %q, "", "")`, selector, nameLabel, name,
		))
	}
	for _, selector := range selectors {
		selector = strings.TrimSpace(selector)
		if strings.HasPrefix(selector, "{") {
			for _, name := range sortedNames {
				if len(sortedNames) == 1 {
					add(selector, name)
					continue
				}
				prefix := strings.TrimSpace(strings.TrimSuffix(selector, "}"))
				if prefix != "{" && !strings.HasSuffix(prefix, ",") {
					prefix += ","
				}
				add(prefix+"__name__="+strconv.Quote(name)+"}", name)
			}
		} else {
			name := strings.SplitN(selector, "{", 2)[0]
			add(selector, name)
		}
	}
	return strings.Join(expressions, " or "), nil
}

func applyPromQLSourceTimestamps(samples, timestamps []promQLSample, nameLabel string) error {
	bySeries := make(map[string]promQLSample, len(timestamps))
	for _, timestamp := range timestamps {
		name, exists := timestamp.labels[nameLabel]
		if !exists || !prometheusMetricNamePattern.MatchString(name) {
			return errors.New("source timestamp response is missing the original metric name")
		}
		delete(timestamp.labels, nameLabel)
		key := promQLSeriesKey(name, timestamp.labels)
		if _, exists := bySeries[key]; exists {
			return errors.New("source timestamp response contains duplicate series")
		}
		bySeries[key] = timestamp
	}
	if len(samples) != len(bySeries) {
		return errors.New("source timestamp response does not match the metric series")
	}
	for i := range samples {
		timestamp, exists := bySeries[promQLSeriesKey(samples[i].name, samples[i].labels)]
		if !exists || timestamp.timestampMillis != samples[i].timestampMillis {
			return errors.New("source timestamp response does not match the metric series and evaluation time")
		}
		var seconds string
		if err := json.Unmarshal(timestamp.rawValue, &seconds); err != nil {
			return fmt.Errorf("invalid source sample timestamp value: %w", err)
		}
		sourceTime, err := parsePromQLTimestamp(json.RawMessage(seconds))
		if err != nil {
			return fmt.Errorf("invalid source sample timestamp: %w", err)
		}
		if sourceTime > samples[i].timestampMillis {
			return errors.New("source sample timestamp is later than its evaluation time")
		}
		samples[i].timestampMillis = sourceTime
	}
	return nil
}

func promQLSeriesKey(name string, labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for label := range labels {
		names = append(names, label)
	}
	sort.Strings(names)
	var key strings.Builder
	key.WriteString(strconv.Quote(name))
	for _, label := range names {
		key.WriteString(strconv.Quote(label))
		key.WriteString(strconv.Quote(labels[label]))
	}
	return key.String()
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

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil) // #nosec G704 -- Origin is administrator-configured and HTTPS-validated; request parameters only populate the query string.
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("failed to create Azure Monitor PromQL request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token.Token)
	request.Header.Set("Accept", "application/json")

	response, err := client.Do(request) // #nosec G704 -- Request targets the administrator-configured origin, not a caller-supplied URL.
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, http.StatusGatewayTimeout, fmt.Errorf("azure monitor PromQL request timed out: %w", err)
		}
		return nil, http.StatusBadGateway, fmt.Errorf("azure monitor PromQL request failed: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxPromQLResponseSize+1))
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("failed to read Azure Monitor PromQL response: %w", err)
	}
	if len(body) > maxPromQLResponseSize {
		return nil, http.StatusBadGateway, fmt.Errorf("azure monitor PromQL response exceeds the %d byte limit", maxPromQLResponseSize)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusUnprocessableEntity {
			return nil, http.StatusBadRequest, fmt.Errorf("azure monitor rejected the PromQL query: %s", strings.TrimSpace(string(body)))
		}
		return nil, http.StatusBadGateway, fmt.Errorf("azure monitor PromQL request returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
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
		return "", errors.New("azure monitor workspace Prometheus query endpoint must be an HTTPS origin URL")
	}

	parsed.Path = "/api/v1/query"
	params := url.Values{}
	query, err := expandPromQLMetricList(requestParams.Get("query"))
	if err != nil {
		return "", err
	}
	params.Set("query", query)
	if requestParams.Has("time") {
		if strings.TrimSpace(requestParams.Get("time")) == "" {
			return "", errors.New(`query parameter "time" must not be empty`)
		}
		params.Set("time", requestParams.Get("time"))
	}
	rangeQuery := requestParams.Has("start") || requestParams.Has("end") || requestParams.Has("step")
	if rangeQuery {
		if requestParams.Has("time") {
			return "", errors.New(`query parameter "time" cannot be combined with start, end or step`)
		}
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

func expandPromQLMetricList(query string) (string, error) {
	selectors, err := promQLMetricListSelectors(query)
	if err != nil {
		return "", err
	}
	// Ordinary or ignores metric names when matching otherwise identical labels.
	return strings.Join(selectors, " or on (__name__) "), nil
}

func promQLMetricListSelectors(query string) ([]string, error) {
	selector := strings.TrimSpace(query)
	// 只展开独立选择器，避免在没有完整 PromQL 语法解析的情况下改变嵌套表达式。
	if !promQLStandaloneSelector.MatchString(selector) {
		return []string{query}, nil
	}

	remaining := strings.TrimSpace(selector[1 : len(selector)-1])
	var matchers []string
	var metricNames []string
	nameMatchers := 0
	for remaining != "" {
		match := promQLSelectorMatcher.FindStringSubmatch(remaining)
		if match == nil {
			return nil, errors.New("invalid standalone PromQL selector: expected a label matcher")
		}
		if match[1] == "__name__" {
			nameMatchers++
			if nameMatchers > 1 {
				return nil, errors.New("standalone PromQL selectors support only one __name__ matcher")
			}
			if match[2] == "=~" {
				value, err := strconv.Unquote(match[3])
				if err != nil {
					return nil, fmt.Errorf("invalid PromQL metric list string: %w", err)
				}
				seen := make(map[string]bool)
				for _, name := range strings.Split(value, "|") {
					// 只接受完整指标名的并集，不能把通配符等正则语义误当成字面名称。
					if !prometheusMetricNamePattern.MatchString(name) {
						return nil, errors.New("PromQL metric name regex compatibility supports only a fixed list such as metric_a|metric_b; wildcards, groups, anchors and empty alternatives are not supported")
					}
					if !seen[name] {
						metricNames = append(metricNames, name)
						seen[name] = true
					}
				}
			} else {
				matchers = append(matchers, strings.TrimSpace(match[0]))
			}
		} else {
			matchers = append(matchers, strings.TrimSpace(match[0]))
		}
		remaining = remaining[len(match[0]):]
		if remaining == "" {
			break
		}
		if remaining[0] != ',' {
			return nil, errors.New("invalid standalone PromQL selector: expected a comma between label matchers")
		}
		remaining = strings.TrimSpace(remaining[1:])
	}
	if len(metricNames) == 0 {
		return []string{query}, nil
	}
	suffix := ""
	if len(matchers) > 0 {
		suffix = "{" + strings.Join(matchers, ",") + "}"
	}
	selectors := make([]string, 0, len(metricNames))
	for _, name := range metricNames {
		selectors = append(selectors, name+suffix)
	}
	return selectors, nil
}

func parsePromQLResponse(body []byte) ([]promQLSample, error) {
	var response azurePrometheusResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("invalid Azure Monitor PromQL response: %w", err)
	}
	if response.Status != "success" {
		if response.Error == "" {
			response.Error = "Azure Monitor returned an unsuccessful PromQL response"
		}
		if response.ErrorType != "" {
			return nil, promQLQueryError{message: fmt.Sprintf("%s: %s", response.ErrorType, response.Error)}
		}
		return nil, promQLQueryError{message: response.Error}
	}

	var samples []promQLSample
	switch response.Data.ResultType {
	case "vector", "matrix":
		var result []azurePrometheusVectorSample
		if err := json.Unmarshal(response.Data.Result, &result); err != nil {
			return nil, fmt.Errorf("invalid PromQL %s result: %w", response.Data.ResultType, err)
		}
		for _, vectorSample := range result {
			point := vectorSample.Value
			if response.Data.ResultType == "matrix" {
				if len(vectorSample.Values) == 0 {
					return nil, errors.New("PromQL matrix series does not contain float samples")
				}
				point = vectorSample.Values[len(vectorSample.Values)-1]
			}
			if len(point) != 2 {
				return nil, fmt.Errorf("PromQL %s result does not contain a timestamp and value", response.Data.ResultType)
			}
			timestamp, err := parsePromQLTimestamp(point[0])
			if err != nil {
				return nil, err
			}
			value, err := parsePromQLValue(point[1])
			if err != nil {
				return nil, err
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
			samples = append(samples, promQLSample{name: name, labels: labels, value: value, timestampMillis: timestamp, rawValue: point[1]})
		}
	case "scalar":
		var result []json.RawMessage
		if err := json.Unmarshal(response.Data.Result, &result); err != nil {
			return nil, fmt.Errorf("invalid PromQL scalar result: %w", err)
		}
		if len(result) != 2 {
			return nil, errors.New("PromQL scalar result does not contain a timestamp and value")
		}
		timestamp, err := parsePromQLTimestamp(result[0])
		if err != nil {
			return nil, err
		}
		value, err := parsePromQLValue(result[1])
		if err != nil {
			return nil, err
		}
		samples = append(samples, promQLSample{name: promQLResultMetricName, labels: map[string]string{}, value: value, timestampMillis: timestamp, rawValue: result[1]})
	default:
		return nil, fmt.Errorf("unsupported PromQL result type %q; vector, scalar and matrix results are supported", response.Data.ResultType)
	}

	return samples, nil
}

func parsePromQLTimestamp(raw json.RawMessage) (int64, error) {
	number := strings.TrimSpace(string(raw))
	if len(number) == 0 || len(number) > 128 || !json.Valid(raw) ||
		(number[0] != '-' && (number[0] < '0' || number[0] > '9')) {
		return 0, errors.New("PromQL timestamp must be a Unix timestamp in numeric seconds")
	}
	if i := strings.IndexAny(number, "eE"); i >= 0 {
		exponent, err := strconv.Atoi(number[i+1:])
		if err != nil || exponent < -100 || exponent > 100 {
			return 0, errors.New("PromQL timestamp exponent is out of range")
		}
	}
	seconds, ok := new(big.Rat).SetString(number)
	if !ok {
		return 0, errors.New("invalid numeric PromQL timestamp")
	}
	milliseconds := new(big.Rat).Mul(seconds, big.NewRat(1000, 1))
	integer, remainder := new(big.Int), new(big.Int)
	integer.QuoRem(milliseconds.Num(), milliseconds.Denom(), remainder)
	if remainder.Sign() < 0 {
		integer.Sub(integer, big.NewInt(1))
	}
	if !integer.IsInt64() {
		return 0, errors.New("PromQL timestamp exceeds the supported millisecond range")
	}
	return integer.Int64(), nil
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
		labels, err := normalizePromQLLabels(sample.labels)
		if err != nil {
			return "", fmt.Errorf("PromQL metric %q: %w", sample.name, err)
		}
		for labelName := range labels {
			if !prometheusLabelNamePattern.MatchString(labelName) {
				return "", fmt.Errorf("PromQL result contains invalid Prometheus label name %q", labelName)
			}
		}
		sample.labels = labels
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
			fmt.Fprintf(&output, " %s %d\n", strconv.FormatFloat(sample.value, 'g', -1, 64), sample.timestampMillis)
		}
	}
	return output.String(), nil
}

func normalizePromQLLabels(labels map[string]string) (map[string]string, error) {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)

	normalized := make(map[string]string, len(labels))
	originalNames := make(map[string]string, len(labels))
	for _, name := range names {
		normalizedName := strings.Map(func(char rune) rune {
			if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
				char >= '0' && char <= '9' || char == '_' {
				return char
			}
			return '_'
		}, name)
		if normalizedName == "" || normalizedName[0] >= '0' && normalizedName[0] <= '9' {
			normalizedName = "_" + normalizedName
		}
		if normalizedName == "__name__" {
			return nil, fmt.Errorf("label %q normalizes to reserved label name %q", name, normalizedName)
		}
		if originalName, exists := originalNames[normalizedName]; exists {
			return nil, fmt.Errorf("labels %q and %q normalize to the same label name %q", originalName, name, normalizedName)
		}
		originalNames[normalizedName] = name
		normalized[normalizedName] = labels[name]
	}
	return normalized, nil
}

func escapePrometheusLabelValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return replacer.Replace(value)
}
