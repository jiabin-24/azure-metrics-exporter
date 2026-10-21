package metricdata

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestWindowRoundTrip(t *testing.T) {
	timestamp := time.Date(2026, 10, 21, 22, 0, 0, 123000000, time.FixedZone("local", 8*60*60))
	window := Window{
		SchemaVersion: SchemaVersion, Source: Source,
		Metrics: []Series{{
			Name: "cpu", Labels: map[string]string{"resourceID": "/resources/example"},
			Samples: []Sample{{Timestamp: timestamp, Value: 1.2345678901234567}},
		}},
	}
	body, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `["2026-10-21T14:00:00.123Z",1.2345678901234567]`) {
		t.Fatalf("sample must be a UTC timestamp/number tuple: %s", body)
	}
	got, err := Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	sample := got.Metrics[0].Samples[0]
	if !sample.Timestamp.Equal(timestamp) || sample.Value != window.Metrics[0].Samples[0].Value {
		t.Fatalf("sample changed: %+v", sample)
	}
}

func TestDecodeRejectsInvalidWindow(t *testing.T) {
	valid := `{"schemaVersion":"1.0","source":"azure-monitor","metrics":[{"name":"cpu","labels":{},"samples":[["2026-10-21T14:00:00Z",1]]}]}`
	cases := map[string]string{
		"non JSON":          "not JSON",
		"empty object":      `{}`,
		"unsupported":       strings.Replace(valid, `"1.0"`, `"2.0"`, 1),
		"wrong source":      strings.Replace(valid, `"azure-monitor"`, `"other"`, 1),
		"unknown field":     strings.Replace(valid, `"name":"cpu"`, `"name":"cpu","promql":"up"`, 1),
		"missing name":      strings.Replace(valid, `"name":"cpu",`, "", 1),
		"null labels":       strings.Replace(valid, `"labels":{}`, `"labels":null`, 1),
		"null samples":      strings.Replace(valid, `[["2026-10-21T14:00:00Z",1]]`, `null`, 1),
		"wrong tuple size":  strings.Replace(valid, `["2026-10-21T14:00:00Z",1]`, `["2026-10-21T14:00:00Z",1,2]`, 1),
		"null value":        strings.Replace(valid, `,1]`, `,null]`, 1),
		"string value":      strings.Replace(valid, `,1]`, `,"1"]`, 1),
		"overflow value":    strings.Replace(valid, `,1]`, `,1e10000]`, 1),
		"negative overflow": strings.Replace(valid, `,1]`, `,-1e10000]`, 1),
		"NaN value":         strings.Replace(valid, `,1]`, `,NaN]`, 1),
		"invalid time":      strings.Replace(valid, `"2026-10-21T14:00:00Z"`, `"yesterday"`, 1),
		"null time":         strings.Replace(valid, `"2026-10-21T14:00:00Z"`, `null`, 1),
		"trailing JSON":     valid + `{}`,
		"trailing garbage":  valid + `bad`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode([]byte(body)); err == nil {
				t.Fatal("expected a schema validation error")
			}
		})
	}
	if _, err := Decode([]byte(`{"schemaVersion":"1.0","source":"azure-monitor","metrics":[]}`)); err != nil {
		t.Fatalf("empty results are valid: %v", err)
	}
}

func TestInvalidSamplesCannotBeEncoded(t *testing.T) {
	for _, sample := range []Sample{
		{Value: 1},
		{Timestamp: time.Now(), Value: math.NaN()},
		{Timestamp: time.Now(), Value: math.Inf(1)},
	} {
		if _, err := json.Marshal(sample); err == nil {
			t.Fatalf("expected invalid sample to fail: %+v", sample)
		}
	}
}
