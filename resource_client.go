package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/patrickmn/go-cache"
	"github.com/webdevops/go-common/azuresdk/armclient"

	"github.com/webdevops/azure-metrics-exporter/internal/requestgroup"
)

type azureResourceClient struct {
	credential azcore.TokenCredential
	options    *arm.ClientOptions
	cache      *cache.Cache
	providers  requestgroup.Group[*armresources.Provider]
}

func newAzureResourceClient(credential azcore.TokenCredential, options *arm.ClientOptions) (*azureResourceClient, error) {
	ttl := 6 * time.Hour
	if value := os.Getenv(armclient.EnvVarServiceDiscoveryTtl); value != "" {
		var err error
		ttl, err = time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", armclient.EnvVarServiceDiscoveryTtl, err)
		}
	}
	return &azureResourceClient{
		credential: credential, options: options.Clone(), cache: cache.New(ttl, time.Minute),
	}, nil
}

func (c *azureResourceClient) GetCachedResource(ctx context.Context, id string) (*armresources.GenericResourceExpanded, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id = strings.ToLower(strings.TrimSpace(id))
	cacheKey := "resource:" + id
	if value, ok := c.cache.Get(cacheKey); ok {
		return value.(*armresources.GenericResourceExpanded), nil
	}
	resourceID, err := arm.ParseResourceID(id)
	if err != nil {
		return nil, fmt.Errorf("parse Azure resource ID: %w", err)
	}
	if resourceID.SubscriptionID == "" || resourceID.ResourceType.Namespace == "" || resourceID.ResourceType.Type == "" {
		return nil, fmt.Errorf("azure resource ID must identify a subscription-scoped resource: %q", id)
	}
	provider, err := c.getProvider(ctx, resourceID.SubscriptionID, resourceID.ResourceType.Namespace)
	if err != nil {
		return nil, err
	}
	version, err := azureResourceAPIVersion(provider, resourceID.ResourceType.Type)
	if err != nil {
		return nil, err
	}
	client, err := armresources.NewClient(resourceID.SubscriptionID, c.credential, c.options)
	if err != nil {
		return nil, fmt.Errorf("create Azure resource client: %w", err)
	}
	response, err := client.GetByID(ctx, id, version, nil)
	if err != nil {
		return nil, fmt.Errorf("get Azure resource by ID: %w", err)
	}
	resource := response.GenericResource
	result := &armresources.GenericResourceExpanded{
		ID: resource.ID, Name: resource.Name, Type: resource.Type,
		Location: resource.Location, Tags: resource.Tags, Properties: resource.Properties,
		ExtendedLocation: resource.ExtendedLocation, Identity: resource.Identity,
		Kind: resource.Kind, ManagedBy: resource.ManagedBy, Plan: resource.Plan, SKU: resource.SKU,
	}
	c.cache.SetDefault(cacheKey, result)
	return result, nil
}

func (c *azureResourceClient) getProvider(ctx context.Context, subscription, namespace string) (*armresources.Provider, error) {
	key := "provider:" + subscription + "/" + namespace
	if value, ok := c.cache.Get(key); ok {
		return value.(*armresources.Provider), nil
	}
	return c.providers.Do(ctx, key, azureResourceLookupTimeout, func(ctx context.Context) (*armresources.Provider, error) {
		if value, ok := c.cache.Get(key); ok {
			return value.(*armresources.Provider), nil
		}
		client, err := armresources.NewProvidersClient(subscription, c.credential, c.options)
		if err != nil {
			return nil, fmt.Errorf("create Azure resource provider client: %w", err)
		}
		response, err := client.Get(ctx, namespace, nil)
		if err != nil {
			return nil, fmt.Errorf("get Azure resource provider metadata: %w", err)
		}
		c.cache.SetDefault(key, &response.Provider)
		return &response.Provider, nil
	})
}

func azureResourceAPIVersion(provider *armresources.Provider, resourceType string) (string, error) {
	for _, candidate := range provider.ResourceTypes {
		if candidate == nil || candidate.ResourceType == nil || !strings.EqualFold(*candidate.ResourceType, resourceType) {
			continue
		}
		var stable, latest string
		for _, version := range candidate.APIVersions {
			if version == nil || *version == "" {
				continue
			}
			if *version > latest {
				latest = *version
			}
			if _, err := time.Parse("2006-01-02", *version); err == nil && *version > stable {
				stable = *version
			}
		}
		if stable != "" {
			return stable, nil
		}
		if latest != "" {
			return latest, nil
		}
	}
	return "", fmt.Errorf("azure provider has no API version for resource type %q", resourceType)
}
