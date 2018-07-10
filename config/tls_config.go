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

package config

import (
	"crypto/tls"

	"io/ioutil"

	"crypto/x509"
)

// TLSConfig is TLS configuration for client.
// TLSConfig has tls config embedded in it so that users can set any field
// of tls config as they wish. TLS config also has some helpers such as SetCaPath, AddClientCertAndKeyPath to
// make configuration easier for users.
type TLSConfig struct {
	baseTLSConfig *tls.Config
	enabled       bool
}

// NewTLSConfig returns BaseTLSConfig.
func NewTLSConfig() *TLSConfig {
	return &TLSConfig{
		baseTLSConfig: new(tls.Config),
	}
}

// SetCaPath sets CA file path.
func (tc *TLSConfig) SetCaPath(path string) error {
	// load CA cert
	caCert, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tc.baseTLSConfig.RootCAs = caCertPool
	return nil
}

// AddClientCertAndKeyPath adds client certificate path and client key path to tls config.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
func (tc *TLSConfig) AddClientCertAndKeyPath(clientCertPath string, clientKeyPath string) error {
	cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return err
	}
	if len(tc.baseTLSConfig.Certificates) == 0 {
		tc.baseTLSConfig.Certificates = []tls.Certificate{cert}
	} else {
		tc.baseTLSConfig.Certificates = append(tc.baseTLSConfig.Certificates, cert)
	}
	return nil
}

// SetInsecureSkipVerify sets InsecureSkipVerify field of tls config.
// InsecureSkipVerify controls whether a client verifies the
// server's certificate chain and host name.
// If InsecureSkipVerify is true, TLS accepts any certificate
// presented by the server and any host name in that certificate.
// In this mode, TLS is susceptible to man-in-the-middle attacks.
// This should be used only for testing.
func (tc *TLSConfig) SetInsecureSkipVerify(enabled bool) {
	tc.baseTLSConfig.InsecureSkipVerify = enabled
}

// SetBaseTLSConfig sets base tls config as the given one.
func (tc *TLSConfig) SetBaseTLSConfig(cfg *tls.Config) {
	tc.baseTLSConfig = cfg
}

// BaseTLSConfig returns base tls config.
func (tc *TLSConfig) BaseTLSConfig() *tls.Config {
	return tc.baseTLSConfig
}

// SetServerName sets the server name for tls config.
func (tc *TLSConfig) SetServerName(serverName string) {
	tc.baseTLSConfig.ServerName = serverName
}

// SetEnabled sets enabled field.
// In order to use TLS connection, this field should be enabled.
func (tc *TLSConfig) SetEnabled(enabled bool) {
	tc.enabled = enabled
}

// Enabled returns true if TLS is enabled, false otherwise.
func (tc *TLSConfig) Enabled() bool {
	return tc.enabled
}
