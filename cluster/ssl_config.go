/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// SSLConfig is SSL configuration for client.
// SSLConfig has tls.Config embedded in it so that users can set any field of tls config as they wish.
type SSLConfig struct {
	tlsConfig *tls.Config
	// ServerName sets the host name of the server.
	ServerName string
	Enabled    bool `json:",omitempty"`
}

func (c *SSLConfig) Clone() SSLConfig {
	c.ensureTLSConfig()
	return SSLConfig{
		ServerName: c.ServerName,
		Enabled:    c.Enabled,
		tlsConfig:  c.tlsConfig.Clone(),
	}
}

func (c *SSLConfig) Validate() error {
	c.ensureTLSConfig()
	return nil
}

// SetTLSConfig overrides the internal TLS configuration.
// Use this method only when you want to replace the internal TLS configuration.
// It should be called before calling any other methods or setting any fields.
func (c *SSLConfig) SetTLSConfig(tlsConfig *tls.Config) {
	c.tlsConfig = tlsConfig.Clone()
}

// TLSConfig returns the clone of internal TLS configuration.
func (c *SSLConfig) TLSConfig() *tls.Config {
	c.ensureTLSConfig()
	return c.tlsConfig.Clone()
}

// SetCAPath sets CA file path.
func (c *SSLConfig) SetCAPath(path string) error {
	c.ensureTLSConfig()
	// XXX: what happens if the path is loaded multiple times?
	// load CA cert
	if caCert, err := os.ReadFile(path); err != nil {
		return ihzerrors.NewIOError("reading CA certificate: %w", err)
	} else {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return ihzerrors.NewIOError("error while loading the CA file, make sure the path exits and the format is pem", nil)
		} else {
			c.tlsConfig.RootCAs = caCertPool
		}
	}
	return nil
}

// AddClientCertAndKeyPath adds client certificate path and client private key path to tls config.
// The files in the given paths must contain PEM encoded data.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (c *SSLConfig) AddClientCertAndKeyPath(clientCertPath string, clientPrivateKeyPath string) error {
	c.ensureTLSConfig()
	if cert, err := tls.LoadX509KeyPair(clientCertPath, clientPrivateKeyPath); err != nil {
		return fmt.Errorf("loading key pair: %w", err)
	} else {
		c.tlsConfig.Certificates = append(c.tlsConfig.Certificates, cert)
	}
	return nil
}

// AddClientCertAndEncryptedKeyPath decrypts the keyfile with the given password and
// adds client certificate path and the decrypted client private key to tls config.
// The files in the given paths must contain PEM encoded data.
// The key file should have a DEK-info header otherwise an error will be returned.
// In order to add multiple client certificate-key pairs one should call this function for each of them.
// If certificates is empty then no certificate will be sent to
// the server. If this is unacceptable to the server then it may abort the handshake.
// For mutual authentication at least one client certificate should be added.
// It returns an error if any of files cannot be loaded.
func (c *SSLConfig) AddClientCertAndEncryptedKeyPath(certPath string, privateKeyPath string, password string) error {
	c.ensureTLSConfig()
	var certPEMBlock, privatePEM, der []byte
	var privKey *rsa.PrivateKey
	var cert tls.Certificate
	var err error
	if certPEMBlock, err = os.ReadFile(certPath); err != nil {
		return fmt.Errorf("reading cert: %w", err)
	}
	if privatePEM, err = os.ReadFile(privateKeyPath); err != nil {
		return fmt.Errorf("reading private key: %w", err)
	}
	privatePEMBlock, _ := pem.Decode(privatePEM)
	if der, err = x509.DecryptPEMBlock(privatePEMBlock, []byte(password)); err != nil {
		return fmt.Errorf("decrypting private key: %w", err)
	}
	if privKey, err = x509.ParsePKCS1PrivateKey(der); err != nil {
		return fmt.Errorf("parsing private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: privatePEMBlock.Type, Bytes: x509.MarshalPKCS1PrivateKey(privKey)})
	if cert, err = tls.X509KeyPair(certPEMBlock, keyPEM); err != nil {
		return fmt.Errorf("creating certificate from key pair: %w", err)
	}
	c.tlsConfig.Certificates = append(c.tlsConfig.Certificates, cert)
	return nil
}

func (c *SSLConfig) ensureTLSConfig() {
	if c.tlsConfig == nil {
		c.tlsConfig = &tls.Config{
			ServerName: c.ServerName,
		}
	}
}
