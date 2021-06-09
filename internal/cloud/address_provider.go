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
	addrs []pubcluster.Address
	dc    *DiscoveryClient
}

func NewAddressProvider(dc *DiscoveryClient, addrs []Address) (*AddressProvider, error) {
	if pubAddrs, err := translateAddrs(addrs); err != nil {
		return nil, err
	} else {
		return &AddressProvider{
			addrs: pubAddrs,
			dc:    dc,
		}, nil
	}
}

func (a *AddressProvider) Addresses(refresh bool) []pubcluster.Address {
	if refresh {
		if err := a.refresh(); err != nil {
			// TODO: log the error
			return nil
		}
	}
	return a.addrs
}

func (a *AddressProvider) refresh() error {
	addrs, err := a.dc.DiscoverNodes(context.Background())
	if err != nil {
		return err
	}
	pubAddrs, err := translateAddrs(addrs)
	if err != nil {
		return err
	}
	a.addrs = pubAddrs
	return nil
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
