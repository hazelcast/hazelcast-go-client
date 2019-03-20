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

// +build enterprise

package ssl

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSLAuthenticationClientRunning(t *testing.T) {
	clusterID, err := createMemberWithXML("hazelcast-ssl.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	sslCfg.SetEnabled(true)
	sslCfg.SetCaPath(server1CA)
	sslCfg.ServerName = serverName
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	require.NoError(t, err)
	assert.Equal(t, client.LifecycleService().IsRunning(), true)
}

func TestSSLAuthenticationMapTest(t *testing.T) {
	clusterID, err := createMemberWithXML("hazelcast-ssl.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	sslCfg.SetEnabled(true)
	sslCfg.SetCaPath(server1CA)
	sslCfg.ServerName = serverName
	client, _ := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, _ := client.GetMap("testMap")
	mp.Put("key", "value")
	val, err := mp.Get("key")
	require.NoError(t, err)
	assert.Equalf(t, val, "value", "mp.Get returned a wrong value")
}
