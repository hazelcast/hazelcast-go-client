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

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
)

type AddressProvider struct {
	dc *DiscoveryClient
}

func NewAddressProvider(dc *DiscoveryClient) *AddressProvider {
	return &AddressProvider{dc: dc}
}

func (a *AddressProvider) Addresses() []pubcluster.Address {
	addrs, err := a.dc.DiscoverNodes(context.Background())
	if err != nil {
		// TODO: log the error
		return nil
	}
	pubAddrs, err := translateAddrs(addrs)
	if err != nil {
		// TODO: log the error
		return nil
	}
	return pubAddrs
}

func translateAddrs(addrs []Address) ([]pubcluster.Address, error) {
	pubAddrs := make([]pubcluster.Address, len(addrs))
	for i, addr := range addrs {
		if pubAddr, err := cluster.ParseAddress(addr.Public); err != nil {
			return nil, err
		} else {
			pubAddrs[i] = pubAddr
		}
	}
	return pubAddrs, nil
}
