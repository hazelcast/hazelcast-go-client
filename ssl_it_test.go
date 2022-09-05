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

package hazelcast_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestSecureSocketLayer(t *testing.T) {
	tests := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "mutualAuthentication", f: mutualAuthenticationTests},
	}
	for _, test := range tests {
		t.Run(test.name, test.f)
	}
}

type MutualAuthenticationStatus string

const (
	// represents unset mutual authentication status property
	MutualAuthenticationStatusNone     MutualAuthenticationStatus = "NONE"
	MutualAuthenticationStatusOptional MutualAuthenticationStatus = "OPTIONAL"
	MutualAuthenticationStatusRequired MutualAuthenticationStatus = "REQUIRED"
)

func mutualAuthenticationTests(t *testing.T) {
	testCases := []struct {
		name          string
		muStatus      MutualAuthenticationStatus
		ca, cert, key string
		wantErr       bool
	}{
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest.whenRequired
			name:     "whenRequired",
			muStatus: MutualAuthenticationStatusRequired,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  false,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenRequired_andServerNotAuthenticated
			name:     "whenRequired_andServerNotAuthenticated",
			muStatus: MutualAuthenticationStatusRequired,
			ca:       "testdata/unknown-server-cert.pem",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  true,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenRequired_andWrongClientAuthenticated
			name:     "whenRequired_andWrongClientAuthenticated",
			muStatus: MutualAuthenticationStatusRequired,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "testdata/unknown-client-cert.pem",
			key:      "testdata/unknown-client-key.pem",
			wantErr:  true,
		},
		{
			// both server and client are not known by the root certificate authority
			name:     "whenRequired_BothClientAndServerUnknownByRootCA",
			muStatus: MutualAuthenticationStatusRequired,
			ca:       "testdata/unknown-server-cert.pem",
			cert:     "testdata/unknown-client-cert.pem",
			key:      "testdata/unknown-client-key.pem",
			wantErr:  true,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenOptional
			name:     "whenOptional",
			muStatus: MutualAuthenticationStatusOptional,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  false,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenOptional_andServerNotAuthenticated
			name:     "whenOptional_andServerNotAuthenticated",
			muStatus: MutualAuthenticationStatusOptional,
			ca:       "testdata/unknown-server-cert.pem",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  true,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenOptional_andWrongClientAuthenticated
			name:     "whenOptional_andWrongClientAuthenticated",
			muStatus: MutualAuthenticationStatusOptional,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "testdata/unknown-client-cert.pem",
			key:      "testdata/unknown-client-key.pem",
			wantErr:  true,
		},
		{
			// both server and client are not known by the root certificate authority
			name:     "whenOptional_BothClientAndServerUnknownByRootCA",
			muStatus: MutualAuthenticationStatusOptional,
			ca:       "testdata/unknown-server-cert.pem",
			cert:     "testdata/unknown-client-cert.pem",
			key:      "testdata/unknown-client-key.pem",
			wantErr:  true,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenNone_andServerAuthenticated
			name:     "whenNone_andServerAuthenticated",
			muStatus: MutualAuthenticationStatusNone,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  false,
		},
		{
			// ported from, com.hazelcast.client.nio.ssl.ClientAuthenticationTest#whenNone_andServerNotAuthenticated
			name:     "whenNone_andServerNotAuthenticated",
			muStatus: MutualAuthenticationStatusNone,
			ca:       "testdata/unknown-server-cert.pem",
			cert:     "testdata/openssl/client/client.crt",
			key:      "testdata/openssl/client/client.key",
			wantErr:  true,
		},
		{
			// status is required, certificate and key are not given by the client
			name:     "whenRequired_NoCertificateFromTheClient",
			muStatus: MutualAuthenticationStatusRequired,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "",
			key:      "",
			wantErr:  true,
		},
		{
			// status is optional, certificate and key are not given by the client
			name:     "whenOptional_NoCertificateFromTheClient",
			muStatus: MutualAuthenticationStatusOptional,
			ca:       "testdata/openssl/server/server.crt",
			cert:     "",
			key:      "",
			wantErr:  false,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.wantErr {
				require.Error(t, sslTestUtil(t.Name(), tc.muStatus, tc.ca, tc.cert, tc.key))
			} else {
				require.NoError(t, sslTestUtil(t.Name(), tc.muStatus, tc.ca, tc.cert, tc.key))
			}
		})
	}
}

func sslTestUtil(clusterName string, status MutualAuthenticationStatus, ca, cert, key string) error {
	ctx := context.Background()
	port := it.NextPort()
	clsConfig := xmlSSLMutualAuthenticationConfig(clusterName, port, status)
	tc := it.StartNewClusterWithConfig(1, clsConfig, port)
	defer tc.Shutdown()
	clientCfg := hz.Config{}
	clientCfg.Cluster.Name = clusterName
	clientCfg.Cluster.Network.SetAddresses(fmt.Sprintf("localhost:%d", tc.Port))
	clientCfg.Cluster.Network.SSL.Enabled = true
	clientCfg.Cluster.Network.SSL.SetTLSConfig(&tls.Config{ServerName: "test.hazelcast.com"})
	if err := clientCfg.Cluster.Network.SSL.SetCAPath(ca); err != nil {
		return err
	}
	// unset certificate and key should not be added
	if len(cert) != 0 && len(key) != 0 {
		if err := clientCfg.Cluster.Network.SSL.AddClientCertAndKeyPath(cert, key); err != nil {
			return err
		}
	}
	// invalid certificates cause client spinning on reconnection to cluster
	startClientCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	client, err := hz.StartNewClientWithConfig(startClientCtx, clientCfg)
	if err != nil {
		return err
	}
	if err = client.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func xmlSSLMutualAuthenticationConfig(clusterName string, port int, status MutualAuthenticationStatus) string {
	var muProperty string
	if status != MutualAuthenticationStatusNone {
		muProperty = fmt.Sprintf("<property name=\"javax.net.ssl.mutualAuthentication\">%s</property>", status)
	}
	return fmt.Sprintf(`
		<hazelcast xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://www.hazelcast.com/schema/config
           http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
			<cluster-name>%s</cluster-name>
			<network>
				<port>%d</port>
				<ssl enabled="true">
					<factory-class-name>
						com.hazelcast.nio.ssl.ClasspathSSLContextFactory
					</factory-class-name>
					<properties>
						<property name="keyStore">%s</property>
						<property name="keyStorePassword">123456</property>
						<property name="trustStore">%s</property>
						<property name="trustStorePassword">123456</property>
						<property name="trustManagerAlgorithm">SunX509</property>
						%s
						<property name="keyManagerAlgorithm">SunX509</property>
						<property name="protocol">TLSv1.2</property>
					</properties>
				</ssl>
			</network>
		</hazelcast>
			`, clusterName, port, "testdata/openssl/server.keystore", "testdata/openssl/server.truststore", muProperty)
}
