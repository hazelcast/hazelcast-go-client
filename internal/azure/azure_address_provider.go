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

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cloud"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type AddressProvider struct {
	config   *pubcluster.AzureConfig
	azClient *Client
	addrs    []*pubcluster.AddressImpl
}

func NewAddressProvider(config *pubcluster.AzureConfig, logger logger.Logger) (*AddressProvider, error) {
	azClient := NewClient(config, logger)
	addrs, err := azClient.GetAddrs(context.Background())
	if err != nil {
		return nil, err
	}
	ap := &AddressProvider{
		config:   config,
		azClient: azClient,
	}
	ap.addrs = ap.makeAddrs(addrs)
	return ap, nil
}

func (a AddressProvider) Addresses() []*pubcluster.AddressImpl {
	return a.addrs
}

func (a AddressProvider) makeAddrs(addrs []cloud.Address) []*pubcluster.AddressImpl {
	r := []*pubcluster.AddressImpl{}
	pr := a.config.PortRange()
	usePublic := a.config.UsePublicIP
	for _, addr := range addrs {
		host := addr.Private
		if usePublic {
			host = addr.Public
		}
		for port := pr.Begin; port <= pr.End; port++ {
			r = append(r, pubcluster.NewAddress(host, int32(port)))
		}
	}
	return r
}
