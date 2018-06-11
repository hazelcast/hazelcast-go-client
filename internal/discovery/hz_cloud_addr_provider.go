// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discovery

import (
	"log"

	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/iputil"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

// HzCloudAddrProvider provides initial addresses for hazelcast.cloud
type HzCloudAddrProvider struct {
	cloudDiscovery *HazelcastCloud
}

// NewHzCloudAddrProvider returns a HzCloudAddrProvider with the given parameters.
func NewHzCloudAddrProvider(cloudToken string, connectionTimeout time.Duration) *HzCloudAddrProvider {
	return NewHzCloudAddrProviderWithCloudDisc(
		NewHazelcastCloud(
			cloudToken,
			connectionTimeout,
		),
	)
}

// NewHzCloudAddrProviderWithCloudDisc returns a HzCloudAddrProvider with the given parameters.
func NewHzCloudAddrProviderWithCloudDisc(cloudDisc *HazelcastCloud) *HzCloudAddrProvider {
	return &HzCloudAddrProvider{
		cloudDiscovery: cloudDisc,
	}
}

// LoadAddresses returns a slice of addresses.
func (ap *HzCloudAddrProvider) LoadAddresses() []core.Address {
	privateToPublicAddrs, err := ap.cloudDiscovery.discoverNodes()
	if err != nil {
		log.Println("Failed to load addresses from hazelcast.cloud ", err)
	}
	addrSlice := make([]core.Address, 0)
	// Appends private keys
	for address := range privateToPublicAddrs {
		ip, port := iputil.GetIPAndPort(address)
		addrSlice = append(addrSlice, proto.NewAddressWithParameters(ip, port))
	}
	return addrSlice
}
