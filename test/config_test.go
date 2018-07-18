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

package test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

func TestSetGroupConfig(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	cfg := hazelcast.NewConfig()
	groupCfg := config.NewGroupConfig()
	groupCfg.SetName("wrongName")
	groupCfg.SetPassword("wrongPassword")
	cfg.SetGroupConfig(groupCfg)
	client, err := hazelcast.NewClientWithConfig(cfg)
	if _, ok := err.(*core.HazelcastAuthenticationError); !ok {
		t.Fatal("client should have returned an authentication error")
	}
	client.Shutdown()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestSetNetworkConfig(t *testing.T) {
	var expected int32 = 10
	cfg := hazelcast.NewConfig()
	nCfg := config.NewNetworkConfig()
	nCfg.SetConnectionAttemptLimit(10)
	cfg.SetNetworkConfig(nCfg)
	actual := cfg.NetworkConfig().ConnectionAttemptLimit()
	assert.Equal(t, nil, expected, actual)
}
