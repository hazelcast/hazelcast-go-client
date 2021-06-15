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

package cloud

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/rest"
)

const envCoordinatorBaseURL = "HZ_CLOUD_COORDINATOR_BASE_URL"

type DiscoveryClient struct {
	logger     logger.Logger
	httpClient *rest.HTTPClient
	token      string
}

func NewDiscoveryClient(config *cluster.HazelcastCloudConfig, logger logger.Logger) *DiscoveryClient {
	return &DiscoveryClient{
		token:      config.Token,
		httpClient: rest.NewHTTPClient(),
		logger:     logger,
	}
}

func (c *DiscoveryClient) DiscoverNodes(ctx context.Context) ([]Address, error) {
	url := makeCoordinatorURL(c.token)
	if j, err := c.httpClient.GetJSONArray(ctx, url); err != nil {
		return nil, err
	} else {
		addrs := extractAddresses(j)
		c.logger.Trace(func() string { return fmt.Sprintf("cloud addresses: %v", addrs) })
		return addrs, nil
	}
}

func extractAddresses(j interface{}) []Address {
	// sample JSON:
	// [{"private-address":"100.115.50.221","public-address":"35.177.212.248:31984"},{"private-address":"100.109.198.133","public-address":"3.8.123.82:31984"}]
	jv := rest.JsonArray(j)
	r := make([]Address, len(jv))
	for i, v := range jv {
		public := rest.JsonString(rest.JsonObjectGet(v, "public-address"))
		private := rest.JsonString(rest.JsonObjectGet(v, "private-address"))
		private, public = normalizePrivatePublicAddr(private, public)
		r[i] = NewAddress(public, private)
	}
	return r
}

func makeCoordinatorURL(token string) string {
	return fmt.Sprintf("%s/cluster/discovery?token=%s", baseURL(), token)
}

func baseURL() string {
	url := os.Getenv(envCoordinatorBaseURL)
	if url == "" {
		return "https://coordinator.hazelcast.cloud"
	}
	return url
}

func normalizePrivatePublicAddr(private, public string) (string, string) {
	var ok bool
	privateHost, privatePort := parseAddress(private)
	publicHost, publicPort := parseAddress(public)

	privatePort, ok = nonZeroOf(privatePort, publicPort)
	if !ok {
		panic("cloud discovery: neither private nor public address have valid ports")
	}
	publicPort, ok = nonZeroOf(publicPort, privatePort)
	if !ok {
		panic("cloud discovery: neither private nor public address have valid ports")
	}

	private = fmt.Sprintf("%s:%d", privateHost, privatePort)
	public = fmt.Sprintf("%s:%d", publicHost, publicPort)
	return private, public
}

func nonZeroOf(choice1, choice2 int) (int, bool) {
	if choice1 > 0 {
		return choice1, true
	}
	if choice2 > 0 {
		return choice2, true
	}
	return 0, false
}

func parseAddress(addr string) (string, int) {
	idx := strings.Index(addr, ":")
	if idx < 0 {
		return addr, 0
	}
	host := addr[:idx]
	port, err := strconv.Atoi(addr[idx+1:])
	if err != nil {
		panic(fmt.Sprintf("invalid cloud member address: %s", addr))
	}
	return host, port
}
