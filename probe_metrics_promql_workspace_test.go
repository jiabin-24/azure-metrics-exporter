package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestResolvePromQLWorkspace(t *testing.T) {
	workspaces := map[string]string{"a": "https://a.example.com", "b": "https://b.example.com"}
	for _, params := range []url.Values{
		nil, {"workspace": {""}}, {"workspace": {"unknown"}}, {"workspace": {"*"}},
		{"workspace": {"a", "b"}}, {"workspace": {"https://other.example.com"}},
	} {
		if _, status, err := resolvePromQLWorkspace(params, workspaces); status != 400 || err == nil {
			t.Fatalf("invalid request accepted: %v status=%d err=%v", params, status, err)
		}
	}
	if _, status, err := resolvePromQLWorkspace(url.Values{"workspace": {"a"}}, nil); status != 503 || err == nil {
		t.Fatal("unconfigured workspaces must return 503")
	}
	single := map[string]string{"a": workspaces["a"]}
	if _, status, err := resolvePromQLWorkspace(nil, single); status != 400 || err == nil {
		t.Fatal("single workspace must still require an explicit alias")
	}
	if got, status, err := resolvePromQLWorkspace(url.Values{"workspace": {"a"}}, single); status != 200 || err != nil || got != single["a"] {
		t.Fatalf("single workspace route failed: %q status=%d err=%v", got, status, err)
	}
}

func TestPromQLWorkspacesHandler(t *testing.T) {
	previous := prometheusWorkspaces
	t.Cleanup(func() { prometheusWorkspaces = previous })
	prometheusWorkspaces = map[string]string{"b": "https://b.example.com", "a": "https://a.example.com"}
	recorder := httptest.NewRecorder()
	promQLWorkspacesHandler(recorder, httptest.NewRequest(http.MethodGet, "/probe/metrics/promql/workspaces", nil))
	var result struct {
		Workspaces []string `json:"workspaces"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || !reflect.DeepEqual(result.Workspaces, []string{"a", "b"}) ||
		!strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") ||
		strings.Contains(recorder.Body.String(), "example.com") {
		t.Fatalf("unexpected workspace inventory: %+v", recorder)
	}
	recorder = httptest.NewRecorder()
	promQLWorkspacesHandler(recorder, httptest.NewRequest(http.MethodPost, "/probe/metrics/promql/workspaces", nil))
	if recorder.Code != http.StatusMethodNotAllowed || recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatal("workspace inventory must only accept GET")
	}
	prometheusWorkspaces = nil
	recorder = httptest.NewRecorder()
	promQLWorkspacesHandler(recorder, httptest.NewRequest(http.MethodGet, "/probe/metrics/promql/workspaces", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatal("unconfigured workspace inventory must return 503")
	}
}

func TestWorkspaceRequestsReachSelectedEndpoint(t *testing.T) {
	for _, workspace := range []string{"a", "b"} {
		t.Run(workspace, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("workspace") || r.URL.Query().Has("format") {
					t.Error("local parameters leaked to Azure")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"cluster":"` + workspace + `"},"value":[1792404000,"1"]}]}}`))
			}))
			defer server.Close()
			params := url.Values{"workspace": {workspace}, "query": {"sum(up)"}, "format": {"json"}}
			endpoint, _, err := resolvePromQLWorkspace(params, map[string]string{workspace: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			body, status, err := collectPromQLMetrics(context.Background(), endpoint, params, promQLWindowCredential{}, server.Client(), nil)
			if err != nil || status != 200 || !strings.Contains(body, `"cluster":"`+workspace+`"`) {
				t.Fatalf("selected query failed: %s status=%d err=%v", body, status, err)
			}
			if params.Get("workspace") != workspace {
				t.Fatal("routing mutated caller parameters")
			}
		})
	}
}
