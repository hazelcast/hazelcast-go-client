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

	"github.com/hazelcast/hazelcast-go-client/internal/cloud"

	"github.com/hazelcast/hazelcast-go-client/internal/http"
)

const networkInterfaceURLFormat = "%%s/subscriptions/%%s/resourceGroups/%%s/providers/Microsoft.Network/%s?api-version=%%s"
const networkInterfaceScaleSetURLFormat = "%%s/subscriptions/%%s/resourceGroups/%%s/providers/Microsoft.Compute/virtualMachineScaleSets/%%s/%s?api-version=%%s"

type ComputeAPI struct {
	endpoint   string
	httpClient *http.Client
}

func NewComputeAPI(client *http.Client) *ComputeAPI {
	return NewComputeAPIWithEndpoint(client, apiEndpoint)
}

func NewComputeAPIWithEndpoint(client *http.Client, endpoint string) *ComputeAPI {
	return &ComputeAPI{
		endpoint:   endpoint,
		httpClient: client,
	}
}

func (c *ComputeAPI) Instances(ctx context.Context, subscriptionID, resourceGroup, scaleSet, tag, accessToken string) ([]cloud.Address, error) {
	// TODO: fetch JSON concurrently
	j, err := c.getJSON(ctx, c.urlForPrivateIPList(subscriptionID, resourceGroup, scaleSet), accessToken)
	if err != nil {
		return nil, err
	}
	networkInterfaces := ExtractPrivateIPs(j)
	j, err = c.getJSON(ctx, c.urlForPublicIPList(subscriptionID, resourceGroup, scaleSet), accessToken)
	if err != nil {
		return nil, err
	}
	publicIPs := ExtractPublicIPs(j)
	addrs := make([]cloud.Address, 0, len(networkInterfaces))
	for _, ni := range networkInterfaces {
		if tag != "" && !ni.HasTag(tag) {
			continue
		}
		addrs = append(addrs, cloud.Address{
			Public:  publicIPs[ni.PublicIPID],
			Private: ni.PrivateIP,
		})
	}
	return addrs, nil
}

func ExtractPrivateIPs(j map[string]interface{}) map[string]NetworkInterface {
	r := map[string]NetworkInterface{}
	for _, item := range jsonArray(j["value"]) {
		tags := extractTags(item)
		ns := extractIpConfigurations(item)
		for _, n := range ns {
			if n.PublicIPID == "" {
				continue
			}
			n.Tags = tags
			r[n.PublicIPID] = n
		}
	}
	return r
}

func ExtractPublicIPs(j map[string]interface{}) map[string]string {
	r := map[string]string{}
	for _, item := range jsonArray(j["value"]) {
		id := jsonString(jsonObjectGet(item, "id"))
		ip := jsonString(jsonObjectGet(jsonObjectGet(item, "properties"), "ipAddress"))
		if id != "" && ip != "" {
			r[id] = ip
		}
	}
	return r
}

func (c *ComputeAPI) urlForPrivateIPList(subscriptionID, resourceGroup, scaleSet string) string {
	if scaleSet == "" {
		return fmt.Sprintf(MakeNetworkInterfaceURLFormat("networkInterfaces"), c.endpoint, subscriptionID, resourceGroup, apiVersion)
	}
	return fmt.Sprintf(MakeNetworkInterfaceScaleSetURLFormat("networkInterfaces"), c.endpoint, subscriptionID, resourceGroup, scaleSet, apiVersionScaleSet)
}

func (c *ComputeAPI) urlForPublicIPList(subscriptionID, resourceGroup, scaleSet string) string {
	if scaleSet == "" {
		return fmt.Sprintf(MakeNetworkInterfaceURLFormat("publicIPAddresses"), c.endpoint, subscriptionID, resourceGroup, apiVersion)
	}
	return fmt.Sprintf(MakeNetworkInterfaceScaleSetURLFormat("publicIPAddresses"), c.endpoint, subscriptionID, resourceGroup, scaleSet, apiVersionScaleSet)
}

func (c *ComputeAPI) getJSON(ctx context.Context, url string, token string) (map[string]interface{}, error) {
	return c.httpClient.GetJSON(ctx, url, http.NewHeader("Authorization", fmt.Sprintf("Bearer %s", token)))
}

func extractTags(j interface{}) map[string]string {
	return jsonStringObject(jsonObjectGet(j, "tags"))
}

func extractIpConfigurations(j interface{}) []NetworkInterface {
	var ns []NetworkInterface
	props := jsonObject(jsonObjectGet(j, "properties"))
	if props == nil {
		return nil
	}
	if jsonObjectGet(props, "virtualMachine") == nil {
		return nil
	}
	for _, ipConfig := range jsonArray(props["ipConfigurations"]) {
		ipProps := jsonObject(jsonObjectGet(ipConfig, "properties"))
		privateIP := jsonString(jsonObjectGet(ipProps, "privateIPAddress"))
		publicIPID := jsonString(jsonObjectGet(jsonObjectGet(ipProps, "publicIPAddress"), "id"))
		ns = append(ns, NetworkInterface{PrivateIP: privateIP, PublicIPID: publicIPID})
	}
	return ns
}

func MakeNetworkInterfaceURLFormat(p string) string {
	return fmt.Sprintf(networkInterfaceURLFormat, p)
}

func MakeNetworkInterfaceScaleSetURLFormat(p string) string {
	return fmt.Sprintf(networkInterfaceScaleSetURLFormat, p)
}
