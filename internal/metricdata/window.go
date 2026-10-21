// Package metricdata 定义 exporter 与 scraper 共用的指标窗口 JSON 协议，不负责查询、缓存或发送数据。
package metricdata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"time"
)

const (
	// SchemaVersion 是窗口 JSON 的协议版本；调整不兼容的数据结构时需要更新版本。
	SchemaVersion = "1.0"
	// Source 标识 Azure Monitor 平台指标 API 的数据。
	Source = "azure-monitor"
	// PrometheusSource 标识 Azure 托管 Prometheus 的 PromQL 查询数据。
	PrometheusSource = "azure-prometheus"
)

// ParseFormat 校验接口的输出格式；未指定时保持原有 Prometheus 文本输出。
// 重复参数、空值及不支持的格式均报错，避免请求被解释成另一种输出格式。
func ParseFormat(params url.Values) (string, error) {
	values, ok := params["format"]
	if !ok {
		return "prometheus", nil
	}
	if len(values) != 1 || (values[0] != "prometheus" && values[0] != "json") {
		return "", fmt.Errorf("parameter \"format\" must have exactly one value: prometheus or json")
	}
	return values[0], nil
}

// Window 是两类 Azure 指标接口共用的 JSON 响应结构，可包含多条时间序列。
// 它只描述返回的数据，不生成时间窗口，也不保证采样点等间隔或填补缺失点。
type Window struct {
	SchemaVersion string   `json:"schemaVersion"`
	Source        string   `json:"source"`
	Metrics       []Series `json:"metrics"`
}

// Series 用指标名和完整标签集标识一条时间序列，Samples 保存该序列返回的采样点。
type Series struct {
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

// Sample 表示一个时间点的数值，JSON 中编码为 [时间字符串, 数值]，而非对象。
// Timestamp 对原始指标表示源采样时间，对 PromQL 表达式结果表示求值时间。
type Sample struct {
	Timestamp time.Time
	Value     float64
}

// MarshalJSON 将时间统一输出为 UTC，并通过标准 JSON 编码拒绝 NaN、Inf 等非有限数值。
func (s Sample) MarshalJSON() ([]byte, error) {
	if s.Timestamp.IsZero() {
		return nil, fmt.Errorf("sample has no source timestamp")
	}
	return json.Marshal([2]any{s.Timestamp.UTC(), s.Value})
}

// UnmarshalJSON 严格解析二元数组，要求时间有效且非零、数值不为 null。
func (s *Sample) UnmarshalJSON(data []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(data, &parts); err != nil {
		return fmt.Errorf("decode sample: %w", err)
	}
	if len(parts) != 2 {
		return fmt.Errorf("sample must contain exactly a timestamp and a numeric value")
	}
	if err := json.Unmarshal(parts[0], &s.Timestamp); err != nil {
		return fmt.Errorf("decode source timestamp: %w", err)
	}
	if s.Timestamp.IsZero() {
		return fmt.Errorf("sample has no source timestamp")
	}
	if bytes.Equal(bytes.TrimSpace(parts[1]), []byte("null")) {
		return fmt.Errorf("sample value must be a number, not null")
	}
	if err := json.Unmarshal(parts[1], &s.Value); err != nil {
		return fmt.Errorf("decode sample value: %w", err)
	}
	return nil
}

// Decode 供 scraper 解析并校验窗口响应；协议或数据不符合约定时返回错误，不接受部分结果。
// 空数组有效，但必需的数组和标签对象不能缺失或为 null。
func Decode(data []byte) (*Window, error) {
	var window Window
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&window); err != nil {
		return nil, fmt.Errorf("decode metrics window JSON: %w", err)
	}
	var extra any
	// 再读一次以确认输入已经结束，拒绝拼接的 JSON 文档或尾部无效内容。
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("metrics window response must contain exactly one JSON document")
	}
	if window.SchemaVersion != SchemaVersion || (window.Source != Source && window.Source != PrometheusSource) {
		return nil, fmt.Errorf("unsupported metrics window schemaVersion or source")
	}
	if window.Metrics == nil {
		return nil, fmt.Errorf("metrics window response must contain a metrics array")
	}
	for _, series := range window.Metrics {
		if series.Name == "" || series.Labels == nil || series.Samples == nil {
			return nil, fmt.Errorf("each metric series must contain a name, labels object and samples array")
		}
	}
	return &window, nil
}
