// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"testing"

	"errors"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/stretchr/testify/assert"
)

var expectedAddresses map[string]*core.Address
var provider *HzCloudAddrProvider
var provider2 *HzCloudAddrProvider

func TestHzCloudAddrProvider(t *testing.T) {
	expectedAddresses = make(map[string]*core.Address)
	expectedAddresses["10.0.0.1:5701"] = core.NewAddressWithParameters("198.51.100.1", 5701)
	expectedAddresses["10.0.0.1:5702"] = core.NewAddressWithParameters("198.51.100.1", 5702)
	expectedAddresses["10.0.0.2:5701"] = core.NewAddressWithParameters("198.51.100.2", 5701)
	var mockProvider = func() (map[string]*core.Address, error) {
		return expectedAddresses, nil
	}

	hazelcastCloudDiscovery := NewHazelcastCloud("", 0, nil)
	hazelcastCloudDiscovery.discoverNodes = mockProvider // mock the discoverNode function

	provider = NewHzCloudAddrProvider("", 0, logger.New())
	provider.cloudDiscovery = hazelcastCloudDiscovery
	provider2 = NewHzCloudAddrProviderWithCloudDisc(hazelcastCloudDiscovery, logger.New())
	testHzCloudAddrProviderLoadAddresses(t)
	testHzCloudAddrProviderLoadAddressesNone(t)
}

func testHzCloudAddrProviderLoadAddresses(t *testing.T) {
	addresses := provider.LoadAddresses()
	assert.Equal(t, 3, len(addresses))
	for key := range expectedAddresses {
		found := false
		for _, addr := range addresses {
			if addr.String() == key {
				found = true
			}
		}
		assert.Equal(t, true, found)
	}
}

func testHzCloudAddrProviderLoadAddressesNone(t *testing.T) {
	var mockProvider2 = func() (map[string]*core.Address, error) {
		return nil, errors.New("error")
	}
	provider2.cloudDiscovery.discoverNodes = mockProvider2
	addresses := provider2.LoadAddresses()
	assert.Equal(t, 0, len(addresses))
}
