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

	hazelcast "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/test/assert"
)

func TestSSLConfigPassword(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, clientKeyPw, password)
	assert.ErrorNil(t, err)
}

func TestSSLConfigInvalidPassword(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, clientKeyPw, "invalid")
	assert.ErrorNotNil(t, err, "invalid password shouldnt decrypt key")
}

func TestSSLConfigPemWithoutDEKInfoHeader(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, client1Key, password)
	assert.ErrorNotNil(t, err, "PEM without DEK-info header should return an error")
}

func TestSSLConfigAddClientCertAndEncryptedKeyPathInvalidCertPath(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath("invalid.txt", client1Key, password)
	assert.ErrorNotNil(t, err, "invalid cert path should return an error")
}

func TestSSLConfigAddClientCertAndEncryptedKeyPathInvalidKeyPath(t *testing.T) {

	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.AddClientCertAndEncryptedKeyPath(clientCertPw, "invalid.txt", password)
	assert.ErrorNotNil(t, err, "invalid cert path should return an error")
}

func TestSSLConfigWrongCAFilePath(t *testing.T) {
	cfg := hazelcast.NewConfig()
	sslCfg := cfg.NetworkConfig().SSLConfig()
	err := sslCfg.SetCaPath("WrongPath.pem")
	assert.ErrorNotNil(t, err, "ssl configuration should fail with wrong CA path")
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
	assert.ErrorNotNil(t, err, "ssl configuration should fail with wrong client cert or key path")
}
