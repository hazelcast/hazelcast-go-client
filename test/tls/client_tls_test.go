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

package tls

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

func TestTLSAuthenticationClientRunning(t *testing.T) {
	if !test.IsEnterprise() {
		t.Skipf("TLS feature requires enterprise version")
	}
	clusterID, err := createMemberWithXML("hazelcast-ssl.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().TLSConfig()
	sslCfg.SetEnabled(true)
	sslCfg.SetCaPath(server1CA)
	sslCfg.SetInsecureSkipVerify(true)
	client, err := hazelcast.NewClientWithConfig(cfg)
	assert.Equal(t, err, client.GetLifecycle().IsRunning(), true)
}

func TestTLSConfigWrongCAFilePath(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().TLSConfig()
	err := sslCfg.SetCaPath("WrongPath.pem")
	assert.ErrorNotNil(t, err, "ssl configuration should fail with wrong CA path")
}

func TestTLSConfigWrongClientCertOrKeyFilePath(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().TLSConfig()
	err := sslCfg.AddClientCertAndKeyPath("WrongPath.pem", "WrongPath.pem")
	assert.ErrorNotNil(t, err, "ssl configuration should fail with wrong client cert or key path")
}

func TestTLSAuthenticationMapTest(t *testing.T) {
	if !test.IsEnterprise() {
		t.Skipf("TLS feature requires enterprise version")
	}
	clusterID, err := createMemberWithXML("hazelcast-ssl.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().TLSConfig()
	sslCfg.SetEnabled(true)
	sslCfg.SetCaPath(server1CA)
	sslCfg.SetInsecureSkipVerify(true)
	client, _ := hazelcast.NewClientWithConfig(cfg)
	mp, _ := client.GetMap("testMap")
	mp.Put("key", "value")
	val, err := mp.Get("key")
	assert.Equalf(t, err, val, "value", "mp.Get returned a wrong value")
}
