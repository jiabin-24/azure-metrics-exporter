package scraper

import (
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/webdevops/azure-metrics-exporter/config"
	"github.com/webdevops/azure-metrics-exporter/internal/metricdata"
	"go.yaml.in/yaml/v2"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	value, err := decodeDuration(unmarshal, false)
	*d = Duration(value)
	return err
}

type NonNegativeDuration time.Duration

func (d *NonNegativeDuration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	value, err := decodeDuration(unmarshal, true)
	*d = NonNegativeDuration(value)
	return err
}

func decodeDuration(unmarshal func(interface{}) error, allowZero bool) (time.Duration, error) {
	var value string
	if err := unmarshal(&value); err != nil {
		return 0, err
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 || (!allowZero && duration == 0) {
		if allowZero {
			return 0, fmt.Errorf("duration must be nonnegative with a unit: %q", value)
		}
		return 0, fmt.Errorf("duration must be positive with a unit: %q", value)
	}
	return duration, nil
}

type Timing struct {
	Interval Duration `yaml:"scrape_interval"`
	Timeout  Duration `yaml:"scrape_timeout"`
}

type Config struct {
	Global   Timing          `yaml:"global"`
	Jobs     []Job           `yaml:"scrape_configs"`
	EventHub *EventHubConfig `yaml:"event_hub"`
}

type EventHubConfig struct {
	Namespace string `yaml:"namespace"`
	Name      string `yaml:"name"`
}

type Job struct {
	Name            string              `yaml:"job_name"`
	Interval        Duration            `yaml:"scrape_interval"`
	Timeout         Duration            `yaml:"scrape_timeout"`
	Scheme          string              `yaml:"scheme"`
	MetricsPath     string              `yaml:"metrics_path"`
	Params          map[string][]string `yaml:"params"`
	HonorTimestamps *bool               `yaml:"honor_timestamps"`
	StaticConfigs   []StaticConfig      `yaml:"static_configs"`
	RollingRange    *RollingRange       `yaml:"rolling_range"`
}

type RollingRange struct {
	Window Duration            `yaml:"window"`
	Step   Duration            `yaml:"step"`
	Delay  NonNegativeDuration `yaml:"delay"`
}

type StaticConfig struct {
	Targets []string          `yaml:"targets"`
	Labels  map[string]string `yaml:"labels"`
}

var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scrape config: %w", err)
	}
	return ParseConfig(data)
}

func ParseConfig(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.UnmarshalStrict(data, cfg); err != nil {
		return nil, fmt.Errorf("parse scrape config: %w", err)
	}
	if cfg.Global.Interval == 0 {
		cfg.Global.Interval = Duration(time.Minute)
	}
	if cfg.Global.Timeout == 0 {
		cfg.Global.Timeout = Duration(10 * time.Second)
		if cfg.Global.Timeout > cfg.Global.Interval {
			cfg.Global.Timeout = cfg.Global.Interval
		}
	}
	if cfg.Global.Timeout > cfg.Global.Interval {
		return nil, fmt.Errorf("global scrape_timeout must not exceed scrape_interval")
	}
	if len(cfg.Jobs) == 0 {
		return nil, fmt.Errorf("scrape_configs must contain at least one job")
	}
	if cfg.EventHub != nil {
		if !regexp.MustCompile(`^[a-zA-Z0-9-]+\.servicebus\.windows\.net$`).MatchString(cfg.EventHub.Namespace) ||
			!regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`).MatchString(cfg.EventHub.Name) {
			return nil, fmt.Errorf("event_hub requires a servicebus.windows.net namespace hostname and a valid hub name")
		}
	}
	names := make(map[string]bool)
	for i := range cfg.Jobs {
		job := &cfg.Jobs[i]
		if job.Name == "" || names[job.Name] {
			return nil, fmt.Errorf("job_name must be nonempty and unique: %q", job.Name)
		}
		names[job.Name] = true
		if job.Interval == 0 {
			job.Interval = cfg.Global.Interval
		}
		if job.Timeout == 0 {
			job.Timeout = cfg.Global.Timeout
			if job.Timeout > job.Interval {
				job.Timeout = job.Interval
			}
		}
		if job.Timeout > job.Interval {
			return nil, fmt.Errorf("job %q: scrape_timeout must not exceed scrape_interval", job.Name)
		}
		if job.Scheme == "" {
			job.Scheme = "http"
		}
		if job.Scheme != "http" && job.Scheme != "https" {
			return nil, fmt.Errorf("job %q: scheme must be http or https", job.Name)
		}
		if job.MetricsPath == "" {
			job.MetricsPath = "/metrics"
		}
		if job.RollingRange != nil {
			if job.MetricsPath != config.ProbeMetricsPromQLUrl ||
				len(job.Params["query"]) != 1 || job.Params["query"][0] == "" {
				return nil, fmt.Errorf("job %q: rolling_range requires /probe/metrics/promql and exactly one query", job.Name)
			}
			for _, key := range []string{"start", "end", "step", "time", "query_delay"} {
				if _, exists := job.Params[key]; exists {
					return nil, fmt.Errorf("job %q: rolling_range cannot be combined with params.%s", job.Name, key)
				}
			}
			rolling := job.RollingRange
			if rolling.Window == 0 {
				rolling.Window = Duration(time.Minute)
			}
			if rolling.Step == 0 {
				rolling.Step = Duration(15 * time.Second)
			}
			if time.Duration(rolling.Window)%time.Millisecond != 0 ||
				time.Duration(rolling.Step)%time.Millisecond != 0 ||
				time.Duration(rolling.Delay)%time.Millisecond != 0 {
				return nil, fmt.Errorf("job %q: rolling_range durations must use whole milliseconds", job.Name)
			}
			if rolling.Step > rolling.Window {
				return nil, fmt.Errorf("job %q: rolling_range.step must not exceed window", job.Name)
			}
		}
		if cfg.EventHub != nil {
			source := eventHubSource(job.MetricsPath)
			if source == "" || len(job.Params["format"]) != 1 || job.Params["format"][0] != "json" {
				return nil, fmt.Errorf("job %q: Event Hub export requires a supported metrics probe endpoint and format=json", job.Name)
			}
			if source == metricdata.PrometheusSource && (len(job.Params["query"]) != 1 || job.Params["query"][0] == "") {
				return nil, fmt.Errorf("job %q: PromQL Event Hub export requires exactly one query", job.Name)
			}
		}
		path, err := url.Parse(job.MetricsPath)
		if err != nil || path == nil || path.IsAbs() || path.Host != "" ||
			path.RawQuery != "" || path.Fragment != "" || job.MetricsPath[0] != '/' {
			return nil, fmt.Errorf("job %q: metrics_path must be an absolute path without query or fragment", job.Name)
		}
		if job.HonorTimestamps != nil && !*job.HonorTimestamps {
			return nil, fmt.Errorf("job %q: honor_timestamps=false is unsupported; source timestamps are always preserved", job.Name)
		}
		targets := make(map[string]bool)
		for _, group := range job.StaticConfigs {
			if len(group.Targets) == 0 {
				return nil, fmt.Errorf("job %q: static_configs entry must contain targets", job.Name)
			}
			for label := range group.Labels {
				if !labelName.MatchString(label) {
					return nil, fmt.Errorf("job %q: invalid label name %q", job.Name, label)
				}
			}
			for _, target := range group.Targets {
				u, err := url.Parse(job.Scheme + "://" + target)
				if err != nil || u == nil || u.Host == "" || u.Host != target ||
					u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
					u.Hostname() == "" || targets[target] {
					return nil, fmt.Errorf("job %q: invalid or duplicate target %q (expected host[:port])", job.Name, target)
				}
				if port := u.Port(); port != "" {
					if _, err := net.LookupPort("tcp", port); err != nil {
						return nil, fmt.Errorf("job %q: invalid target port in %q: %w", job.Name, target, err)
					}
				}
				targets[target] = true
			}
		}
		if len(targets) == 0 {
			return nil, fmt.Errorf("job %q: static_configs must contain targets", job.Name)
		}
	}
	return cfg, nil
}

func (j Job) targetURL(target string) string {
	u := url.URL{Scheme: j.Scheme, Host: target, Path: j.MetricsPath}
	// Parse the path so existing percent-escapes are not escaped a second time.
	path, _ := url.Parse(j.MetricsPath)
	u.Path, u.RawPath = path.Path, path.RawPath
	u.RawQuery = url.Values(j.Params).Encode()
	return u.String()
}

func (j Job) paramsAt(now time.Time) map[string][]string {
	if j.RollingRange == nil {
		return j.Params
	}
	params := maps.Clone(j.Params)
	end := now.UTC().Add(-time.Duration(j.RollingRange.Delay)).Truncate(time.Millisecond)
	start := end.Add(-time.Duration(j.RollingRange.Window))
	params["start"] = []string{start.Format(time.RFC3339Nano)}
	params["end"] = []string{end.Format(time.RFC3339Nano)}
	params["step"] = []string{time.Duration(j.RollingRange.Step).String()}
	return params
}

func eventHubSource(path string) string {
	switch path {
	case config.ProbeMetricsPromQLUrl:
		return metricdata.PrometheusSource
	case config.ProbeMetricsResourceUrl, config.ProbeMetricsListUrl, config.ProbeMetricsSubscriptionUrl,
		config.ProbeMetricsScrapeUrl, config.ProbeMetricsResourceGraphUrl:
		return metricdata.Source
	default:
		return ""
	}
}
