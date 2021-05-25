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

	"github.com/hazelcast/hazelcast-go-client/internal/logger"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/http"
)

type Client struct {
	*http.Client
	metadataAPI   *MetadataAPI
	authenticator *Authenticator
	config        *cluster.AzureConfig
	logger        logger.Logger
}

func NewClient(config *cluster.AzureConfig, logger logger.Logger) *Client {
	client := http.NewClient()
	return &Client{
		Client:        client,
		metadataAPI:   NewMetadataAPI(client),
		authenticator: NewAuthenticator(client),
		config:        config,
		logger:        logger,
	}
}

/*
func (c *Client) getAddrsUsingMetadata(ctx context.Context) ([]cloud.Address, error) {
	var addrs []cloud.Address
	c.logger.Trace(func() string { return "fetching access token" })
	tok, err := c.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.metadataAPI.FetchMetadata(ctx); err != nil {
		return nil, err
	}

}

*/

func (c *Client) accessToken(ctx context.Context) (string, error) {
	if c.config.InstanceMetadataAvailable {
		return c.metadataAPI.AccessToken(ctx)
	}
	return c.authenticator.RefreshAccessToken(ctx, c.config.TenantID, c.config.ClientID, c.config.ClientSecret)
}

/*
func (c *Client) subscriptionID(ctx context.Context) (string, error) {
	if c.config.SubscriptionID != "" {
		return c.config.SubscriptionID, nil
	}
	return c.metadataAPI.SubscriptionID(ctx), nil
}


*/
