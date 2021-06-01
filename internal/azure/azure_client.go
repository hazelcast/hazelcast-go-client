/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package azure

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cloud"
	"github.com/hazelcast/hazelcast-go-client/internal/http"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const apiEndpoint = "https://management.azure.com"

type Client struct {
	logger         logger.Logger
	metadataAPI    *MetadataAPI
	authenticator  *Authenticator
	config         *cluster.AzureConfig
	computeAPI     *ComputeAPI
	subscriptionID string
	resourceGroup  string
	scaleSet       string
	ready          bool
}

func NewClient(config *cluster.AzureConfig, logger logger.Logger) *Client {
	client := http.NewClient()
	return &Client{
		metadataAPI:   NewMetadataAPI(client, logger),
		authenticator: NewAuthenticator(client, logger),
		computeAPI:    NewComputeAPI(client, logger),
		config:        config,
		logger:        logger,
	}
}

func (c Client) GetAddrs(ctx context.Context) ([]cloud.Address, error) {
	if err := c.ensureReady(ctx); err != nil {
		return nil, err
	}
	return c.getAddrs(ctx)
}

func (c *Client) ensureReady(ctx context.Context) error {
	if c.ready {
		return nil
	}
	if err := c.ensureSubscriptionID(ctx); err != nil {
		return err
	}
	if err := c.ensureResourceGroup(ctx); err != nil {
		return err
	}
	if err := c.ensureScaleSet(ctx); err != nil {
		return err
	}
	return nil
}

func (c *Client) ensureSubscriptionID(ctx context.Context) error {
	if c.config.SubscriptionID != "" {
		c.subscriptionID = c.config.SubscriptionID
	} else if err := c.metadataAPI.FetchMetadata(ctx); err != nil {
		return err
	}
	c.subscriptionID = c.metadataAPI.SubscriptionID()
	return nil
}

func (c *Client) ensureResourceGroup(ctx context.Context) error {
	if c.config.ResourceGroup != "" {
		c.resourceGroup = c.config.ResourceGroup
	} else if err := c.metadataAPI.FetchMetadata(ctx); err != nil {
		return err
	}
	c.resourceGroup = c.metadataAPI.ResourceGroup()
	return nil
}

func (c *Client) ensureScaleSet(ctx context.Context) error {
	if c.config.ScaleSet != "" {
		c.scaleSet = c.config.ScaleSet
	} else if err := c.metadataAPI.FetchMetadata(ctx); err != nil {
		return err
	}
	c.scaleSet = c.metadataAPI.ScaleSet()
	return nil
}

func (c *Client) getAddrs(ctx context.Context) ([]cloud.Address, error) {
	tok, err := c.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	c.logger.Trace(func() string { return "fetching metadata" })
	if err = c.metadataAPI.FetchMetadata(ctx); err != nil {
		return nil, err
	}
	sid := c.subscriptionID
	rg := c.resourceGroup
	c.logger.Trace(func() string { return "retrieving addresses" })
	if addrs, err := c.computeAPI.Instances(ctx, c.subscriptionID, rg, c.scaleSet, c.config.Tag, tok); err != nil {
		return nil, err
	} else {
		c.logger.Debug(func() string {
			return fmt.Sprintf("found the following instances for project %s and zone %s: %v", sid, rg, addrs)
		})
		return addrs, nil
	}
}

func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.logger.Trace(func() string { return "getting access token" })
	if c.config.InstanceMetadataAvailable {
		c.logger.Trace(func() string { return "retrieving access token from metadata" })
		tok, err := c.metadataAPI.AccessToken(ctx)
		if err == nil {
			return tok, nil
		}
	}
	c.logger.Trace(func() string { return "refreshing access token" })
	return c.authenticator.RefreshAccessToken(ctx, c.config.TenantID, c.config.ClientID, c.config.ClientSecret)
}
