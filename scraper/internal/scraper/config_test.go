package scraper

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAndURL(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
global:
  scrape_interval: 2m
  scrape_timeout: 30s
scrape_configs:
  - job_name: promql
    scrape_interval: 5s
    metrics_path: /probe/metrics/promql
    params:
      query: ['sum(rate(requests_total{cluster="prod"}[5m]))', 'up']
      query_delay: ["20m"]
    static_configs:
      - targets: ["exporter:8080", "[::1]:8080"]
        labels:
          environment: prod
  - job_name: defaults
    static_configs:
      - targets: ["exporter"]
`))
	if err != nil {
		t.Fatal(err)
	}
	job := cfg.Jobs[0]
	if time.Duration(job.Interval) != 5*time.Second || time.Duration(job.Timeout) != 5*time.Second {
		t.Fatalf("incorrect inherited timing: %+v", job)
	}
	u, err := url.Parse(job.targetURL("exporter:8080"))
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "http" || u.Host != "exporter:8080" || u.Path != "/probe/metrics/promql" ||
		len(u.Query()["query"]) != 2 || u.Query().Get("query") != job.Params["query"][0] ||
		u.Query().Get("query_delay") != "20m" {
		t.Fatalf("incorrect target URL: %s", u)
	}
	if cfg.Jobs[1].MetricsPath != "/metrics" || cfg.Jobs[1].Interval != cfg.Global.Interval ||
		cfg.Jobs[1].Timeout != cfg.Global.Timeout {
		t.Fatalf("incorrect defaults: %+v", cfg.Jobs[1])
	}
}

func TestConfigRejectsInvalidInput(t *testing.T) {
	base := "scrape_configs:\n  - job_name: test\n    static_configs:\n      - targets: ['exporter:8080']\n"
	tests := map[string]string{
		"empty":            "",
		"unknown field":    base + "    relabel_configs: []\n",
		"duplicate key":    base + "    job_name: other\n",
		"zero interval":    base + "    scrape_interval: 0s\n",
		"negative timeout": base + "    scrape_timeout: -1s\n",
		"invalid duration": base + "    scrape_interval: 1d\n",
		"timeout too long": base + "    scrape_interval: 1s\n    scrape_timeout: 2s\n",
		"invalid scheme":   base + "    scheme: ftp\n",
		"relative path":    base + "    metrics_path: metrics\n",
		"path query":       base + "    metrics_path: /metrics?query=up\n",
		"global timing":    "global:\n  scrape_interval: 1s\n  scrape_timeout: 2s\n" + base,
		"duplicate jobs":   base + strings.TrimPrefix(base, "scrape_configs:\n"),
		"missing name":     strings.Replace(base, "job_name: test", "job_name: ''", 1),
		"no targets":       strings.Replace(base, "['exporter:8080']", "[]", 1),
		"full URL target":  strings.Replace(base, "exporter:8080", "http://exporter:8080", 1),
		"target path":      strings.Replace(base, "exporter:8080", "exporter:8080/metrics", 1),
		"target port":      strings.Replace(base, "exporter:8080", "exporter:99999", 1),
		"target auth":      strings.Replace(base, "exporter:8080", "user:pass@exporter:8080", 1),
		"duplicate target": strings.Replace(base, "['exporter:8080']", "['exporter:8080', 'exporter:8080']", 1),
		"invalid label":    base + "        labels: {'bad-label': prod}\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestConfigHonorTimestamps(t *testing.T) {
	const base = "scrape_configs:\n  - job_name: test\n    metrics_path: /probe/metrics/promql\n    static_configs:\n      - targets: [localhost]\n"
	tests := []struct {
		name, input string
		want        bool
	}{
		{"default", base, true},
		{"global true", "global:\n  honor_timestamps: true\n" + base, true},
		{"global false", "global:\n  honor_timestamps: false\n" + base, false},
		{"job false", base + "    honor_timestamps: false\n", false},
		{"job true overrides", "global:\n  honor_timestamps: false\n" + base + "    honor_timestamps: true\n", true},
		{"job false overrides", "global:\n  honor_timestamps: true\n" + base + "    honor_timestamps: false\n", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(test.input))
			if err != nil {
				t.Fatal(err)
			}
			job := cfg.Jobs[0]
			u, err := url.Parse(job.targetURL("localhost"))
			if err != nil {
				t.Fatal(err)
			}
			if job.HonorTimestamps == nil || *job.HonorTimestamps != test.want ||
				u.Query().Get("honor_timestamps") != map[bool]string{true: "true", false: "false"}[test.want] {
				t.Fatalf("timestamp policy not inherited or forwarded: %+v %s", job, u)
			}
			if _, exists := job.Params["honor_timestamps"]; exists {
				t.Fatal("targetURL mutated job params")
			}
		})
	}
	for _, input := range []string{
		base + "    honor_timestamps: invalid\n",
		base + "    params:\n      honor_timestamps: [false]\n",
		strings.Replace(base, "/probe/metrics/promql", "/probe/metrics/list", 1) + "    honor_timestamps: false\n",
		"global:\n  honor_timestamps: false\n" + strings.Replace(base, "/probe/metrics/promql", "/metrics", 1),
	} {
		if _, err := ParseConfig([]byte(input)); err == nil {
			t.Fatalf("expected invalid timestamp policy error: %s", input)
		}
	}
}

func TestDefaultGlobalTiming(t *testing.T) {
	cfg, err := ParseConfig([]byte("scrape_configs:\n- job_name: test\n  static_configs:\n  - targets: [localhost]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(cfg.Global.Interval) != time.Minute || time.Duration(cfg.Global.Timeout) != 10*time.Second {
		t.Fatalf("incorrect defaults: %+v", cfg.Global)
	}
}

func TestConfigMetricName(t *testing.T) {
	const base = `
scrape_configs:
- job_name: fragmentation
  metric_name: etcd_db_fragmentation_ratio
  metrics_path: /probe/metrics/promql
  params:
    query: ['1 - (etcd_mvcc_db_total_size_in_use_in_bytes / etcd_mvcc_db_total_size_in_bytes)']
    format: [json]
  static_configs:
  - targets: [localhost:8080]
`
	for _, name := range []string{"etcd_db_fragmentation_ratio", "custom:metric_total", "_metric"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(strings.Replace(base, "metric_name: etcd_db_fragmentation_ratio", "metric_name: "+name, 1)))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Jobs[0].MetricName != name || len(cfg.Jobs[0].Params) != 2 {
				t.Fatalf("metric_name was lost or forwarded as an HTTP parameter: %+v", cfg.Jobs[0])
			}
		})
	}
	tests := map[string]string{
		"invalid name":    strings.Replace(base, "metric_name: etcd_db_fragmentation_ratio", "metric_name: invalid-name", 1),
		"numeric prefix":  strings.Replace(base, "metric_name: etcd_db_fragmentation_ratio", "metric_name: 1metric", 1),
		"whitespace":      strings.Replace(base, "metric_name: etcd_db_fragmentation_ratio", "metric_name: ' invalid '", 1),
		"platform":        strings.Replace(base, "/probe/metrics/promql", "/probe/metrics/resource", 1),
		"text":            strings.Replace(base, "format: [json]", "format: [prometheus]", 1),
		"missing format":  strings.Replace(base, "    format: [json]\n", "", 1),
		"multiple format": strings.Replace(base, "format: [json]", "format: [json, json]", 1),
		"missing query":   strings.Replace(base, "query: ['1 - (etcd_mvcc_db_total_size_in_use_in_bytes / etcd_mvcc_db_total_size_in_bytes)']", "query: []", 1),
		"empty query":     strings.Replace(base, "query: ['1 - (etcd_mvcc_db_total_size_in_use_in_bytes / etcd_mvcc_db_total_size_in_bytes)']", "query: ['']", 1),
		"multiple query":  strings.Replace(base, "query: ['1 - (etcd_mvcc_db_total_size_in_use_in_bytes / etcd_mvcc_db_total_size_in_bytes)']", "query: [up, other]", 1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(input)); err == nil || !strings.Contains(err.Error(), "metric_name") {
				t.Fatalf("expected metric_name validation error, got %v", err)
			}
		})
	}
}
