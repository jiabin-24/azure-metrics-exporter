package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/patrickmn/go-cache"
	"github.com/webdevops/go-common/azuresdk/armclient"

	"github.com/webdevops/azure-metrics-exporter/internal/requestgroup"
)

const testAzureResourceID = "/subscriptions/sub/resourcegroups/rg/providers/microsoft.containerservice/managedclusters/aks"
const testAzureProviderPath = "/subscriptions/sub/providers/microsoft.containerservice"
const testAzureProvider = `{"namespace":"Microsoft.ContainerService","resourceTypes":[{"resourceType":"managedClusters","apiVersions":["2026-01-01-preview","2024-09-01","2025-01-01"]}]}`
const testAzureResource = `{"id":"` + testAzureResourceID + `","name":"aks","type":"Microsoft.ContainerService/managedClusters","location":"australiaeast","tags":{"environment":"production"},"properties":{"provisioningState":"Succeeded"}}`

func testAzureResourceClient(t *testing.T, handler http.HandlerFunc) *azureResourceClient {
	t.Helper()
	t.Setenv(armclient.EnvVarServiceDiscoveryTtl, "1h")
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	client, err := newAzureResourceClient(promQLWindowCredential{}, &arm.ClientOptions{
		DisableRPRegistration: true,
		ClientOptions: policy.ClientOptions{
			Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
				cloud.ResourceManager: {Endpoint: server.URL, Audience: "https://management.azure.com/"},
			}},
			Transport: server.Client(),
			Retry:     policy.RetryOptions{MaxRetries: -1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestAzureResourceClientDirectLookupCacheAndCoalescing(t *testing.T) {
	var providerCalls, resourceCalls atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("resource lookup must be read-only, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case testAzureProviderPath:
			providerCalls.Add(1)
			fmt.Fprint(w, testAzureProvider)
		case testAzureResourceID:
			resourceCalls.Add(1)
			if r.URL.Query().Get("api-version") != "2025-01-01" {
				t.Errorf("did not select the newest stable API version: %s", r.URL)
			}
			select {
			case <-release:
				fmt.Fprint(w, testAzureResource)
			case <-r.Context().Done():
			}
		default:
			t.Errorf("unexpected request; must not list subscription resources: %s", r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	})
	var lookups azureResourceLookupGroup
	type result struct {
		resource *armresources.GenericResourceExpanded
		err      error
	}
	results := make(chan result, 1)
	ctx := &promQLLookupWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		resource, err := lookups.get(ctx, client, testAzureResourceID)
		results <- result{resource, err}
	}()
	waitForPromQLLookup(t, ctx.waiting)
	for i := range 17 {
		id := testAzureResourceID
		if i%2 == 0 {
			id = strings.ToUpper(id)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		resource, err := lookups.get(ctx, client, id)
		cancel()
		if resource != nil || !errors.Is(err, requestgroup.ErrInFlight) {
			t.Fatalf("later resource requests must skip an in-flight lookup: resource=%v err=%v", resource, err)
		}
	}
	unblock()
	got := waitForPromQLLookup(t, results)
	if got.err != nil || got.resource == nil {
		t.Fatalf("direct lookup failed: %v", got.err)
	}
	first := got.resource
	properties, ok := first.Properties.(map[string]any)
	if first.Location == nil || *first.Location != "australiaeast" ||
		first.Tags["environment"] == nil || *first.Tags["environment"] != "production" ||
		!ok || properties["provisioningState"] != "Succeeded" {
		t.Fatalf("direct lookup lost resource attributes: %+v", first)
	}
	cached, err := lookups.get(t.Context(), client, strings.ToUpper(testAzureResourceID))
	if err != nil || cached != first || providerCalls.Load() != 1 || resourceCalls.Load() != 1 {
		t.Fatalf("warm cache made extra requests: provider=%d resource=%d err=%v", providerCalls.Load(), resourceCalls.Load(), err)
	}

	items := client.cache.Items()
	expired := items["resource:"+testAzureResourceID]
	expired.Expiration = time.Now().Add(-time.Second).UnixNano()
	items["resource:"+testAzureResourceID] = expired
	client.cache = cache.NewFrom(time.Hour, 0, items)
	if _, err := lookups.get(t.Context(), client, testAzureResourceID); err != nil ||
		providerCalls.Load() != 1 || resourceCalls.Load() != 2 {
		t.Fatalf("expired resource should refresh without repeating provider discovery: provider=%d resource=%d err=%v",
			providerCalls.Load(), resourceCalls.Load(), err)
	}
}

func TestAzureResourceClientWarmsCacheAfterEnrichmentWaitTimeout(t *testing.T) {
	if azureResourceWaitTimeout != 5*time.Second || azureResourceLookupTimeout != 30*time.Second {
		t.Fatal("enrichment wait and shared lookup limits must be 5s and 30s respectively")
	}
	var providerCalls, resourceCalls atomic.Int32
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case testAzureProviderPath:
			providerCalls.Add(1)
			fmt.Fprint(w, testAzureProvider)
		case testAzureResourceID:
			resourceCalls.Add(1)
			started <- struct{}{}
			select {
			case <-release:
				fmt.Fprint(w, testAzureResource)
			case <-r.Context().Done():
				cancelled <- struct{}{}
			}
		default:
			t.Errorf("unexpected request: %s", r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	})
	var logs bytes.Buffer
	enricher := promQLRegionEnricher{
		resources: client, logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	samples := []promQLSample{{labels: map[string]string{"resourceID": testAzureResourceID}, value: 7, timestampMillis: 123}}
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		enricher.enrich(t.Context(), samples)
	}()
	waitForPromQLLookup(t, started)
	select {
	case <-done:
	case <-time.After(azureResourceWaitTimeout + 2*time.Second):
		t.Fatal("enrichment kept waiting for the 30-second background request")
	}
	if elapsed := time.Since(start); elapsed < azureResourceWaitTimeout || elapsed > azureResourceWaitTimeout+2*time.Second {
		t.Fatalf("enrichment did not stop at the 5-second wait limit: %v", elapsed)
	}
	if samples[0].labels["region"] != "" || samples[0].value != 7 || samples[0].timestampMillis != 123 ||
		!strings.Contains(logs.String(), "deadline exceeded") {
		t.Fatalf("first scrape must preserve metrics and warn without a region: samples=%+v logs=%s", samples, logs.String())
	}
	select {
	case <-cancelled:
		t.Fatal("enrichment timeout cancelled the shared background resource request")
	default:
	}
	beforeSkip := time.Now()
	skipped := []promQLSample{{labels: map[string]string{"resourceID": testAzureResourceID}, value: 9, timestampMillis: 456}}
	enricher.enrich(t.Context(), skipped)
	if time.Since(beforeSkip) >= time.Second || skipped[0].labels["region"] != "" ||
		skipped[0].value != 9 || skipped[0].timestampMillis != 456 ||
		strings.Count(logs.String(), "level=WARN") != 1 ||
		!strings.Contains(logs.String(), "lookup already in flight; skipping region enrichment") {
		t.Fatalf("later scrape must skip immediately without changing samples or adding WARN: samples=%+v logs=%s",
			skipped, logs.String())
	}
	unblock()
	if _, err := azureResourceLookups.requests.Do(t.Context(), testAzureResourceID, azureResourceLookupTimeout,
		func(ctx context.Context) (*armresources.GenericResourceExpanded, error) {
			return client.GetCachedResource(ctx, testAzureResourceID)
		}); err != nil {
		t.Fatalf("background request failed after the first scrape stopped waiting: %v", err)
	}
	next := []promQLSample{{labels: map[string]string{"resourceID": testAzureResourceID}}}
	enricher.enrich(t.Context(), next)
	if next[0].labels["region"] != "australiaeast" {
		t.Fatalf("later scrape did not receive the background resource result: %+v", next)
	}
	before := time.Now()
	cached := []promQLSample{{labels: map[string]string{"resourceID": testAzureResourceID}}}
	enricher.enrich(t.Context(), cached)
	if cached[0].labels["region"] != "australiaeast" || time.Since(before) >= time.Second ||
		providerCalls.Load() != 1 || resourceCalls.Load() != 1 {
		t.Fatalf("cache was not populated after the initial wait timed out: samples=%+v provider=%d resource=%d",
			cached, providerCalls.Load(), resourceCalls.Load())
	}
}

func TestAzureResourceClientFailuresAreNotCached(t *testing.T) {
	for _, failedPath := range []string{testAzureProviderPath, testAzureResourceID} {
		t.Run(failedPath, func(t *testing.T) {
			var providerCalls, resourceCalls atomic.Int32
			client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var count int32
				switch r.URL.Path {
				case testAzureProviderPath:
					count = providerCalls.Add(1)
				case testAzureResourceID:
					count = resourceCalls.Add(1)
				default:
					t.Errorf("unexpected request: %s", r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				if r.URL.Path == failedPath && count == 1 {
					w.WriteHeader(http.StatusForbidden)
					fmt.Fprint(w, `{"error":{"code":"AuthorizationFailed","message":"denied"}}`)
					return
				}
				if r.URL.Path == testAzureProviderPath {
					fmt.Fprint(w, testAzureProvider)
				} else {
					fmt.Fprint(w, testAzureResource)
				}
			})
			var lookups azureResourceLookupGroup
			_, err := lookups.get(t.Context(), client, testAzureResourceID)
			var responseErr *azcore.ResponseError
			if !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusForbidden {
				t.Fatalf("Azure lookup errors must be returned, got %v", err)
			}
			resource, err := lookups.get(t.Context(), client, testAzureResourceID)
			if err != nil || resource == nil {
				t.Fatalf("failed lookup was cached or could not recover: resource=%v err=%v", resource, err)
			}
			wantProviders, wantResources := int32(1), int32(2)
			if failedPath == testAzureProviderPath {
				wantProviders, wantResources = 2, 1
			}
			if providerCalls.Load() != wantProviders || resourceCalls.Load() != wantResources {
				t.Fatalf("unexpected recovery calls: provider=%d resource=%d", providerCalls.Load(), resourceCalls.Load())
			}
		})
	}
}

func TestAzureResourceClientProviderDiscoveryCoalesces(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != testAzureProviderPath {
			t.Errorf("provider discovery listed unrelated resources: %s", r.URL)
		}
		select {
		case <-release:
			fmt.Fprint(w, testAzureProvider)
		case <-r.Context().Done():
		}
	})
	results := make(chan error, 18)
	for range 18 {
		ctx := &promQLLookupWaitContext{Context: t.Context(), waiting: make(chan struct{})}
		go func() {
			_, err := client.getProvider(ctx, "sub", "microsoft.containerservice")
			results <- err
		}()
		waitForPromQLLookup(t, ctx.waiting)
	}
	unblock()
	for range 18 {
		if err := waitForPromQLLookup(t, results); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("provider discovery must be shared across resource lookups, got %d calls", calls.Load())
	}
}

func TestAzureResourceClientRequestDeadline(t *testing.T) {
	var calls atomic.Int32
	client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == testAzureProviderPath {
			fmt.Fprint(w, testAzureProvider)
			return
		}
		if r.URL.Path != testAzureResourceID {
			t.Errorf("unexpected request: %s", r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, testAzureResource)
	})
	if _, err := client.getProvider(t.Context(), "sub", "microsoft.containerservice"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := client.GetCachedResource(ctx, testAzureResourceID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("direct resource request ignored its deadline: %v", err)
	}
	if _, err := client.GetCachedResource(t.Context(), testAzureResourceID); err != nil || calls.Load() != 2 {
		t.Fatalf("timed out resource lookup must be retried, not cached: calls=%d err=%v", calls.Load(), err)
	}
}

func TestAzureResourceClientCacheTTL(t *testing.T) {
	for _, ttl := range []string{"", "2h", "0s", "-1s", "invalid"} {
		t.Run("ttl="+ttl, func(t *testing.T) {
			t.Setenv(armclient.EnvVarServiceDiscoveryTtl, ttl)
			client, err := newAzureResourceClient(promQLWindowCredential{}, nil)
			if ttl == "invalid" {
				if err == nil {
					t.Fatal("invalid cache TTL must fail initialization")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			client.cache.SetDefault("test", true)
			item := client.cache.Items()["test"]
			if ttl == "0s" || ttl == "-1s" {
				if item.Expiration != 0 {
					t.Fatal("non-expiring cache TTL changed")
				}
				return
			}
			want := time.Hour
			if ttl == "2h" {
				want = 2 * time.Hour
			}
			expiry := time.Unix(0, item.Expiration)
			if expiry.Before(before.Add(want)) || expiry.After(time.Now().Add(want)) {
				t.Fatalf("cache TTL was not applied: expiry=%v ttl=%s", expiry, ttl)
			}
		})
	}
}

func TestAzureResourceAPIVersion(t *testing.T) {
	for _, test := range []struct {
		name, resourceType, want string
		versions                 []string
	}{
		{"prefer stable", "managedClusters", "2025-01-01", []string{"2026-01-01-preview", "2024-01-01", "2025-01-01"}},
		{"preview only", "managedClusters", "2026-01-01-preview", []string{"2025-01-01-preview", "2026-01-01-preview"}},
		{"case insensitive", "MANAGEDCLUSTERS", "2025-01-01", []string{"2025-01-01"}},
		{"missing type", "virtualMachines", "", []string{"2025-01-01"}},
		{"empty versions", "managedClusters", "", []string{""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := "managedClusters"
			resourceType := &armresources.ProviderResourceType{ResourceType: &name, APIVersions: []*string{nil}}
			for _, version := range test.versions {
				resourceType.APIVersions = append(resourceType.APIVersions, &version)
			}
			provider := &armresources.Provider{ResourceTypes: []*armresources.ProviderResourceType{nil, resourceType}}
			version, err := azureResourceAPIVersion(provider, test.resourceType)
			if version != test.want || (err != nil) != (test.want == "") {
				t.Fatalf("unexpected API version: version=%q err=%v", version, err)
			}
		})
	}
}

func TestAzureResourceClientRejectsInvalidIDWithoutRequests(t *testing.T) {
	client := testAzureResourceClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("invalid resource ID made an Azure request: %s", r.URL)
		http.Error(w, "unexpected request", http.StatusBadRequest)
	})
	for _, id := range []string{"invalid", "", "/providers/Microsoft.Management/managementGroups/example"} {
		if _, err := client.GetCachedResource(t.Context(), id); err == nil {
			t.Fatalf("invalid resource ID %q must fail", id)
		}
	}
}
