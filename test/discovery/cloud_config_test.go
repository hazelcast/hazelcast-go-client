// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

	"os"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/discovery"
	"github.com/stretchr/testify/assert"
)

var testDiscoveryToken = "testDiscoveryToken"

func TestCloudConfigDefaults(t *testing.T) {
	cfg := hazelcast.NewConfig()
	cloudConfig := cfg.NetworkConfig().CloudConfig()
	assert.Equalf(t, false, cloudConfig.IsEnabled(), "Default cloud config should be disabled.")
	assert.Equalf(t, "", cloudConfig.DiscoveryToken(), "Default cloud config discovery token"+
		" should be empty.")
}

func TestCloudConfig(t *testing.T) {
	cfg := hazelcast.NewConfig()
	cloudConfig := config.NewCloudConfig()
	cloudConfig.SetEnabled(true)
	cloudConfig.SetDiscoveryToken(testDiscoveryToken)
	cfg.NetworkConfig().SetCloudConfig(cloudConfig)
	returnedCloudCfg := cfg.NetworkConfig().CloudConfig()
	assert.Equalf(t, true, returnedCloudCfg.IsEnabled(), "Cloud discovery should be enabled.")
	assert.Equalf(t, testDiscoveryToken, returnedCloudCfg.DiscoveryToken(), "Cloud discovery token "+
		"should be set.")
}

func TestCloudConfigWithPropertySet(t *testing.T) {
	cloudConfig := config.NewCloudConfig()
	cloudConfig.SetEnabled(true)
	os.Setenv(property.HazelcastCloudDiscoveryToken.Name(), testDiscoveryToken)
	cfg := hazelcast.NewConfig()
	cfg.NetworkConfig().SetCloudConfig(cloudConfig)
	_, err := hazelcast.NewClientWithConfig(cfg)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error("Cloud discovery should have returned an error for both property and client configuration based" +
			" setup")
	}
}

func TestCloudConfigCustomUrlEndpoint(t *testing.T) {
	cfg := hazelcast.NewConfig()
	cfg.SetProperty(discovery.CloudURLBaseProperty.Name(), "https://dev.hazelcast.cloud")
	properties := property.NewHazelcastProperties(cfg.Properties())
	urlEndpoint := discovery.CreateURLEndpoint(properties, "token")
	assert.Equal(t, urlEndpoint, "https://dev.hazelcast.cloud/cluster/discovery?token=token")
}

func TestDefaultCloudUrlEndpoint(t *testing.T) {
	cfg := hazelcast.NewConfig()
	properties := property.NewHazelcastProperties(cfg.Properties())
	urlEndpoint := discovery.CreateURLEndpoint(properties, "token")
	assert.Equal(t, urlEndpoint, "https://coordinator.hazelcast.cloud/cluster/discovery?token=token")
}
