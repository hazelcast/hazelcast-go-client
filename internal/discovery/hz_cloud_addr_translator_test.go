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
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

var lookup map[string]core.Address
var privateAddress core.Address
var publicAddress core.Address
var translator *HzCloudAddrTranslator

func TestHzCloudAddrTranslator_Translate(t *testing.T) {
	lookup = make(map[string]core.Address)
	privateAddress = proto.NewAddressWithParameters("127.0.0.1", 5701)
	publicAddress = proto.NewAddressWithParameters("192.168.0.1", 5701)
	lookup[privateAddress.String()] = publicAddress
	lookup["127.0.0.2:5701"] = proto.NewAddressWithParameters("192.168.0.2", 5701)
	var mockProvider = func() (map[string]core.Address, error) {
		return lookup, nil
	}

	hazelcastCloudDiscovery := NewHazelcastCloud("", 0, nil)
	hazelcastCloudDiscovery.discoverNodes = mockProvider // mock the discoverNode function

	translator = NewHzCloudAddrTranslator("", 0)
	translator.cloudDiscovery = hazelcastCloudDiscovery

	testHzCloudAddrTranslatorTranslateNil(t)
	testHzCloudAddrTranslatorTranslatePrivateToPublic(t)
	testHzCloudAddrTranslatorTranslateWhenNotFoundReturnNil(t)
	testHzCloudAddrTranslatorTranslateAfterRefresh(t)

}

func testHzCloudAddrTranslatorTranslateNil(t *testing.T) {
	if actual := translator.Translate(nil); actual != nil {
		t.Error("hzCloudAddrTranslator.Translate() should return nil for nil address.")
	}
}

func testHzCloudAddrTranslatorTranslatePrivateToPublic(t *testing.T) {
	actual := translator.Translate(privateAddress)
	assert.Equal(t, nil, publicAddress.Host(), actual.Host())
	assert.Equal(t, nil, privateAddress.Port(), actual.Port())
}

func testHzCloudAddrTranslatorTranslateWhenNotFoundReturnNil(t *testing.T) {
	notAvailableAddr := proto.NewAddressWithParameters("127.0.0.3", 5701)

	if actual := translator.Translate(notAvailableAddr); actual != nil {
		t.Error("hzCloudAddTranslator.Translate() should return nil for not found address.")
	}

}

func testHzCloudAddrTranslatorTranslateAfterRefresh(t *testing.T) {
	translator.Refresh()
	actual := translator.Translate(privateAddress)

	assert.Equal(t, nil, publicAddress.Host(), actual.Host())
	assert.Equal(t, nil, privateAddress.Port(), actual.Port())

}
