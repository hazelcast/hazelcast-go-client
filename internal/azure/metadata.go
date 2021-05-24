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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const apiVersion = "2020-09-01"
const metadataEndpoint = "http://169.254.169.254"

type MetadataAPI struct {
	endpoint string
	metadata map[string]interface{}
}

func NewMetadataAPI() *MetadataAPI {
	return NewMetadataAPIWithEndpoint(metadataEndpoint)
}

func NewMetadataAPIWithEndpoint(endpoint string) *MetadataAPI {
	return &MetadataAPI{
		endpoint: endpoint,
		metadata: map[string]interface{}{},
	}
}

func (m *MetadataAPI) FetchMetadata(ctx context.Context) error {
	if len(m.metadata) != 0 {
		return nil
	}
	return m.fetchMetadata(ctx)
}

func (m *MetadataAPI) SubscriptionID() interface{} {
	if s, ok := m.metadata["subscriptionId"]; ok {
		return s
	}
	return ""
}

func (m *MetadataAPI) fetchMetadata(ctx context.Context) error {
	url := fmt.Sprintf("%s/metadata/instance/compute?api-version=%s", m.endpoint, apiVersion)
	b, err := m.fetchURL(ctx, url)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &m.metadata); err != nil {
		return err
	}
	return nil
}

func (m *MetadataAPI) fetchURL(ctx context.Context, url string) ([]byte, error) {
	httpClient := &http.Client{}
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	// error is unhandled
	defer response.Body.Close()
	// handle the response
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}
