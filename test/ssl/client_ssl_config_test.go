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

// +build enterprise

package ssl

import (
	"testing"

	"crypto/tls"

	hazelcast "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSLConfigPassword(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, clientKeyPw, password)
	assert.NoError(t, err)
}

func TestSSLConfigInvalidPassword(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, clientKeyPw, "invalid")
	assert.Errorf(t, err, "invalid password shouldnt decrypt key")
}

func TestSSLConfigPemWithoutDEKInfoHeader(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, client1Key, password)
	assert.Errorf(t, err, "PEM without DEK-info header should return an error")
}

func TestSSLConfigAddClientCertAndEncryptedKeyPathInvalidCertPath(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath("invalid.txt", client1Key, password)
	assert.Errorf(t, err, "invalid cert path should return an error")
}

func TestSSLConfigAddClientCertAndEncryptedKeyPathInvalidKeyPath(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, "invalid.txt", password)
	assert.Errorf(t, err, "invalid cert path should return an error")
}

func TestSSLConfigWrongCAFilePath(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.SetCaPath("WrongPath.pem")
	assert.Errorf(t, err, "ssl configuration should fail with wrong CA path")
}

func TestSSLConfigWithWrongFormatCAFile(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.SetCaPath("invalid-format.txt")
	if _, ok := err.(*core.HazelcastIOError); !ok {
		t.Errorf("SSL Config.SetCaPath should return a HazelcastIOError for invalid file format")
	}
}

func TestSSLConfigWrongClientCertOrKeyFilePath(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndKeyPath("WrongPath.pem", "WrongPath.pem")
	assert.Errorf(t, err, "ssl configuration should fail with wrong client cert or key path")
}

func TestSSLCiphers(t *testing.T) {
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
	sslCfg.CipherSuites = []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA}
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	require.NoError(t, err)
	assert.Equal(t, client.LifecycleService().IsRunning(), true)
}

func TestSSLInvalidCiphers(t *testing.T) {
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
	sslCfg.CipherSuites = []uint16{0}
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	assert.Errorf(t, err, "handshake should fail with invalid cipher suites")
}

func TestSSLProtocolMismatch(t *testing.T) {
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
	sslCfg.MinVersion = tls.VersionTLS12
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	assert.Errorf(t, err, "handshake should fail with mismatched protocol version")
}
