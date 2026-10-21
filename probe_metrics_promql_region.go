package main

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
)

type promQLResourceGetter interface {
	GetCachedResource(context.Context, string) (*armresources.GenericResourceExpanded, error)
}

type promQLRegionEnricher struct {
	resources promQLResourceGetter
	logger    *slog.Logger
}

func promQLResourceID(labels map[string]string) string {
	for _, name := range []string{"microsoft.resourceid", "microsoft_resourceid", "resourceID"} {
		if id := strings.TrimSpace(labels[name]); id != "" {
			return strings.ToLower(id)
		}
	}
	return ""
}

func (e *promQLRegionEnricher) enrich(ctx context.Context, samples []promQLSample) {
	pending := make(map[string]bool)
	for _, sample := range samples {
		if sample.labels["region"] != "" || sample.labels["location"] != "" {
			continue
		}
		if id := promQLResourceID(sample.labels); id != "" {
			pending[id] = true
		}
	}
	if len(pending) == 0 {
		return
	}
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	// Region lookup is optional and must not consume the entire metrics request timeout.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	regions := make(map[string]string, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			e.logger.Warn("PromQL region lookup cancelled; remaining resource regions are not enriched", "error", err)
			break
		}
		resource, err := e.resources.GetCachedResource(ctx, id)
		if err != nil {
			e.logger.Warn("unable to resolve PromQL resource region; preserving metrics without enrichment",
				"resource_id", id, "error", err)
			continue
		}
		if resource == nil || resource.Location == nil || strings.TrimSpace(*resource.Location) == "" {
			e.logger.Warn("PromQL resource or location not found; preserving metrics without enrichment", "resource_id", id)
			continue
		}
		regions[id] = strings.TrimSpace(*resource.Location)
	}
	for _, sample := range samples {
		if sample.labels["region"] == "" && sample.labels["location"] == "" {
			if region := regions[promQLResourceID(sample.labels)]; region != "" {
				sample.labels["region"] = region
			}
		}
	}
}
