/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package cluster_test

import (
	"crypto/tls"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

func TestSSLConfig_SetCAPath(t *testing.T) {
	sslConfig := cluster.SSLConfig{
		Enabled: true,
	}
	for _, tc := range []struct {
		info    string
		caPath  string
		isError bool
	}{
		{info: "invalid certificate authority", caPath: "non-exist-filepath", isError: true},
		{info: "valid certificate authority", caPath: "../testdata/server1-cert.pem", isError: false},
		{info: "another valid certificate authority", caPath: "../testdata/server2-cert.pem", isError: false},
	} {
		t.Run(tc.info, func(t *testing.T) {
			err := sslConfig.SetCAPath(tc.caPath)
			if tc.isError == true && !errors.Is(err, hzerrors.NewIOError(err.Error(), err)) {
				t.Fatalf("error type is incorrect, want %v", hzerrors.NewIOError(err.Error(), err))
			}
		})
	}
}

func TestSSLConfig_AddClientCertAndKeyPath(t *testing.T) {
	var (
		err       error
		tlsConfig *tls.Config
	)
	sslConfig := cluster.SSLConfig{
		Enabled: true,
	}
	t.Log("invalid both client certificate path and key path")
	err = sslConfig.AddClientCertAndKeyPath("non-exist-filepath", "non-exist-filepath")
	require.Error(t, err)
	tlsConfig = sslConfig.TLSConfig()
	require.Equal(t, len(tlsConfig.Certificates), 0)
	t.Log("valid client certificate, key pair")
	err = sslConfig.AddClientCertAndKeyPath("../testdata/client1-cert.pem", "../testdata/client1-key.pem")
	require.NoError(t, err)
	tlsConfig = sslConfig.TLSConfig()
	require.Equal(t, len(tlsConfig.Certificates), 1)
	t.Log("another valid client certificate, key pair")
	err = sslConfig.AddClientCertAndKeyPath("../testdata/client2-cert.pem", "../testdata/client2-key.pem")
	require.NoError(t, err)
	tlsConfig = sslConfig.TLSConfig()
	require.Equal(t, len(tlsConfig.Certificates), 2)
}

func TestSSLConfig_AddClientCertAndEncryptedKeyPath(t *testing.T) {
}
