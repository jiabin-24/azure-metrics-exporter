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
    honor_timestamps: true
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
		"empty":                    "",
		"unknown field":            base + "    relabel_configs: []\n",
		"duplicate key":            base + "    job_name: other\n",
		"zero interval":            base + "    scrape_interval: 0s\n",
		"negative timeout":         base + "    scrape_timeout: -1s\n",
		"invalid duration":         base + "    scrape_interval: 1d\n",
		"timeout too long":         base + "    scrape_interval: 1s\n    scrape_timeout: 2s\n",
		"invalid scheme":           base + "    scheme: ftp\n",
		"relative path":            base + "    metrics_path: metrics\n",
		"path query":               base + "    metrics_path: /metrics?query=up\n",
		"timestamp rewrite":        base + "    honor_timestamps: false\n",
		"global timestamp rewrite": "global:\n  honor_timestamps: false\n" + base,
		"global timing":            "global:\n  scrape_interval: 1s\n  scrape_timeout: 2s\n" + base,
		"duplicate jobs":           base + strings.TrimPrefix(base, "scrape_configs:\n"),
		"missing name":             strings.Replace(base, "job_name: test", "job_name: ''", 1),
		"no targets":               strings.Replace(base, "['exporter:8080']", "[]", 1),
		"full URL target":          strings.Replace(base, "exporter:8080", "http://exporter:8080", 1),
		"target path":              strings.Replace(base, "exporter:8080", "exporter:8080/metrics", 1),
		"target port":              strings.Replace(base, "exporter:8080", "exporter:99999", 1),
		"target auth":              strings.Replace(base, "exporter:8080", "user:pass@exporter:8080", 1),
		"duplicate target":         strings.Replace(base, "['exporter:8080']", "['exporter:8080', 'exporter:8080']", 1),
		"invalid label":            base + "        labels: {'bad-label': prod}\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestConfigGlobalHonorTimestamps(t *testing.T) {
	const base = `
global:
  honor_timestamps: true
scrape_configs:
- job_name: inherited
  static_configs:
  - targets: [localhost]
- job_name: overridden
  honor_timestamps: true
  static_configs:
  - targets: [localhost]
`
	cfg, err := ParseConfig([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range cfg.Jobs {
		if job.HonorTimestamps == nil || !*job.HonorTimestamps {
			t.Fatalf("honor_timestamps was not inherited or overridden: %+v", job)
		}
	}
	if _, err := ParseConfig([]byte(strings.Replace(base, "  honor_timestamps: true\n  static_configs:",
		"  honor_timestamps: false\n  static_configs:", 1))); err == nil {
		t.Fatal("job honor_timestamps=false must remain unsupported")
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
