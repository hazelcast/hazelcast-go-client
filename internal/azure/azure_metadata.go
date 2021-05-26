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

	"github.com/hazelcast/hazelcast-go-client/internal/http"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const (
	metadataAPIVersion = "2020-09-01"
	metadataEndpoint   = "http://169.254.169.254"
)

type MetadataAPI struct {
	endpoint    string
	metadata    map[string]interface{}
	httpClient  *http.Client
	hasMetadata bool
	logger      logger.Logger
}

func NewMetadataAPI(client *http.Client, logger logger.Logger) *MetadataAPI {
	return NewMetadataAPIWithEndpoint(client, logger, metadataEndpoint)
}

func NewMetadataAPIWithEndpoint(client *http.Client, logger logger.Logger, endpoint string) *MetadataAPI {
	return &MetadataAPI{
		endpoint:   endpoint,
		metadata:   map[string]interface{}{},
		httpClient: client,
		logger:     logger,
	}
}

func (m *MetadataAPI) FetchMetadata(ctx context.Context) error {
	if m.hasMetadata {
		return nil
	}
	return m.fetchMetadata(ctx)
}

func (m *MetadataAPI) SubscriptionID() string {
	return jsonStringValue(m.metadata, "subscriptionId")
}

func (m *MetadataAPI) ResourceGroup() string {
	return jsonStringValue(m.metadata, "resourceGroupName")
}

func (m *MetadataAPI) Location() string {
	return jsonStringValue(m.metadata, "location")
}

func (m *MetadataAPI) AvailabilityZone() string {
	return jsonStringValue(m.metadata, "zone")
}

func (m *MetadataAPI) FaultDomain() string {
	return jsonStringValue(m.metadata, "platformFaultDomain")
}

func (m *MetadataAPI) ScaleSet() string {
	return jsonStringValue(m.metadata, "vmScaleSetName")
}

func (m *MetadataAPI) AccessToken(ctx context.Context) (string, error) {
	return m.fetchAccessToken(ctx)
}

func (m *MetadataAPI) fetchMetadata(ctx context.Context) error {
	url := fmt.Sprintf("%s/metadata/instance/compute?api-version=%s", m.endpoint, metadataAPIVersion)
	m.logger.Trace(func() string { return fmt.Sprintf("fetching metadata: %s", url) })
	if metadata, err := getJSON(ctx, m.httpClient, url); err != nil {
		return fmt.Errorf("error fetching metadata: %w", err)
	} else {
		m.metadata = metadata
		m.hasMetadata = true
	}
	return nil
}

func (m *MetadataAPI) fetchAccessToken(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/metadata/identity/oauth2/token?api-version=%s&resource=%s", m.endpoint, metadataAPIVersion, apiEndpoint)
	m.logger.Trace(func() string { return fmt.Sprintf("fetching access token: %s", url) })
	if j, err := getJSON(ctx, m.httpClient, url); err != nil {
		return "", err
	} else if i, ok := j["access_token"]; ok {
		if s, ok := i.(string); ok {
			return s, nil
		}
	}
	return "", nil
}
