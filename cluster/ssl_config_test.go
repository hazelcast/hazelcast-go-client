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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestSSLConfig_SetCAPath(t *testing.T) {
	sslConfig := cluster.SSLConfig{
		Enabled: true,
	}
	testCases := []struct {
		info     string
		caPath   string
		hasError bool
	}{
		{info: "invalid certificate authority", caPath: "non-exist-filepath", hasError: true},
		{info: "valid certificate authority", caPath: "testdata/server1-cert.pem", hasError: false},
		{info: "another valid certificate authority", caPath: "testdata/server2-cert.pem", hasError: false},
	}
	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			err := sslConfig.SetCAPath(tc.caPath)
			if err != nil && !tc.hasError {
				t.Fatal(err)
			} else if err == nil && tc.hasError {
				t.Fatal("got nil want an error")
			}
		})
	}
}

func TestSSLConfig_AddClientCertAndKeyPath(t *testing.T) {
	sslConfig := cluster.SSLConfig{
		Enabled: true,
	}
	// invalid both client certificate path and key path
	err := sslConfig.AddClientCertAndKeyPath("non-exist-filepath", "non-exist-filepath")
	require.Error(t, err)
	require.Equal(t, len(sslConfig.TLSConfig().Certificates), 0)
	// valid client certificate, key pair
	err = sslConfig.AddClientCertAndKeyPath("testdata/client1-cert.pem", "testdata/client1-key.pem")
	require.NoError(t, err)
	require.Equal(t, len(sslConfig.TLSConfig().Certificates), 1)
	// another valid client certificate, key pair
	err = sslConfig.AddClientCertAndKeyPath("testdata/client2-cert.pem", "testdata/client2-key.pem")
	require.NoError(t, err)
	require.Equal(t, len(sslConfig.TLSConfig().Certificates), 2)
}
