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
		"empty":             "",
		"unknown field":     base + "    relabel_configs: []\n",
		"duplicate key":     base + "    job_name: other\n",
		"zero interval":     base + "    scrape_interval: 0s\n",
		"negative timeout":  base + "    scrape_timeout: -1s\n",
		"invalid duration":  base + "    scrape_interval: 1d\n",
		"timeout too long":  base + "    scrape_interval: 1s\n    scrape_timeout: 2s\n",
		"invalid scheme":    base + "    scheme: ftp\n",
		"relative path":     base + "    metrics_path: metrics\n",
		"path query":        base + "    metrics_path: /metrics?query=up\n",
		"timestamp rewrite": base + "    honor_timestamps: false\n",
		"global timing":     "global:\n  scrape_interval: 1s\n  scrape_timeout: 2s\n" + base,
		"duplicate jobs":    base + strings.TrimPrefix(base, "scrape_configs:\n"),
		"missing name":      strings.Replace(base, "job_name: test", "job_name: ''", 1),
		"no targets":        strings.Replace(base, "['exporter:8080']", "[]", 1),
		"full URL target":   strings.Replace(base, "exporter:8080", "http://exporter:8080", 1),
		"target path":       strings.Replace(base, "exporter:8080", "exporter:8080/metrics", 1),
		"target port":       strings.Replace(base, "exporter:8080", "exporter:99999", 1),
		"target auth":       strings.Replace(base, "exporter:8080", "user:pass@exporter:8080", 1),
		"duplicate target":  strings.Replace(base, "['exporter:8080']", "['exporter:8080', 'exporter:8080']", 1),
		"invalid label":     base + "        labels: {'bad-label': prod}\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatal("expected configuration error")
			}
		})
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
