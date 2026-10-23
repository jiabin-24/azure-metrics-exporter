package main

import (
	"context"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/webdevops/azure-metrics-exporter/internal/requestgroup"
)

const (
	azureResourceLookupTimeout = 30 * time.Second
	azureResourceWaitTimeout   = 5 * time.Second
)

var azureResourceLookups azureResourceLookupGroup

type azureResourceGetter interface {
	GetCachedResource(context.Context, string) (*armresources.GenericResourceExpanded, error)
}

type azureResourceLookupGroup struct {
	requests requestgroup.Group[*armresources.GenericResourceExpanded]
}

func (g *azureResourceLookupGroup) get(ctx context.Context, resources azureResourceGetter, id string) (*armresources.GenericResourceExpanded, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	return g.requests.TryDo(ctx, id, azureResourceLookupTimeout, func(ctx context.Context) (*armresources.GenericResourceExpanded, error) {
		return resources.GetCachedResource(ctx, id)
	})
}
