package hazelcast_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

type MutualAuthenticationStatus string

const (
	OPTIONAL MutualAuthenticationStatus = "OPTIONAL"
	REQUIRED MutualAuthenticationStatus = "REQUIRED"
)

func TestMutualAuthentication(t *testing.T) {
	testCases := []struct {
		name          string
		muStatus      MutualAuthenticationStatus
		ca, cert, key string
		wantErr       bool
	}{
		{
			name:     "testMA_RequiredClientAndServerAuthenticated",
			muStatus: REQUIRED,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "cluster/testdata/client1-cert.pem",
			key:      "cluster/testdata/client1-key.pem",
			wantErr:  false,
		},
		{
			name:     "testMA_RequiredServerNotAuthenticated",
			muStatus: REQUIRED,
			ca:       "cluster/testdata/server2-cert.pem",
			cert:     "cluster/testdata/client1-cert.pem",
			key:      "cluster/testdata/client1-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_RequiredClientNotAuthenticated",
			muStatus: REQUIRED,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "cluster/testdata/client2-cert.pem",
			key:      "cluster/testdata/client2-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_RequiredClientAndServerNotAuthenticated",
			muStatus: REQUIRED,
			ca:       "cluster/testdata/server2-cert.pem",
			cert:     "cluster/testdata/client2-cert.pem",
			key:      "cluster/testdata/client2-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_OptionalClientAndServerAuthenticated",
			muStatus: OPTIONAL,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "cluster/testdata/client1-cert.pem",
			key:      "cluster/testdata/client1-key.pem",
			wantErr:  false,
		},
		{
			name:     "testMA_OptionalServerNotAuthenticated",
			muStatus: OPTIONAL,
			ca:       "cluster/testdata/server2-cert.pem",
			cert:     "cluster/testdata/client1-cert.pem",
			key:      "cluster/testdata/client1-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_OptionalClientNotAuthenticated",
			muStatus: OPTIONAL,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "cluster/testdata/client2-cert.pem",
			key:      "cluster/testdata/client2-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_OptionalClientAndServerNotAuthenticated",
			muStatus: OPTIONAL,
			ca:       "cluster/testdata/server2-cert.pem",
			cert:     "cluster/testdata/client2-cert.pem",
			key:      "cluster/testdata/client2-key.pem",
			wantErr:  true,
		},
		{
			name:     "testMA_RequiredWithNoCertFile",
			muStatus: REQUIRED,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "",
			key:      "",
			wantErr:  true,
		},
		{
			name:     "testMA_RequiredWithNoCertFile",
			muStatus: OPTIONAL,
			ca:       "cluster/testdata/server1-cert.pem",
			cert:     "",
			key:      "",
			wantErr:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr {
				require.Error(t, sslTestUtil(t, tc.ca, tc.cert, tc.key))
			} else {
				require.NoError(t, sslTestUtil(t, tc.ca, tc.cert, tc.key))
			}
		})
	}
}

func sslTestUtil(t *testing.T, ca, cert, key string) error {
	ctx := context.Background()
	port := it.NextPort()
	clsConfig := xmlSSLMutualAuthenticationConfig(t.Name(), port, REQUIRED)
	tc := it.StartNewClusterWithConfig(1, clsConfig, port)
	defer tc.Shutdown()
	clientCfg := tc.DefaultConfig()
	if err := clientCfg.Cluster.Network.SSL.SetCAPath(ca); err != nil {
		return err
	}
	if err := clientCfg.Cluster.Network.SSL.AddClientCertAndKeyPath(cert, key); err != nil {
		return err
	}
	client, err := hazelcast.StartNewClientWithConfig(ctx, clientCfg)
	if err != nil {
		return err
	}
	if err = client.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func xmlSSLMutualAuthenticationConfig(clusterName string, port int, mu MutualAuthenticationStatus) string {
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
						<property name="keyStore">com/hazelcast/nio/ssl-mutual-auth/server1.keystore</property>
						<property name="keyStorePassword">password</property>
						<property name="trustStore">com/hazelcast/nio/ssl-mutual-auth/server1_knows_client1/server1.truststore
						</property>
						<property name="trustStorePassword">password</property>
						<property name="trustManagerAlgorithm">SunX509</property>
						<property name="javax.net.ssl.mutualAuthentication">%s</property>
						<property name="keyManagerAlgorithm">SunX509</property>
						<property name="protocol">TLSv1.2</property>
					</properties>
				</ssl>
			</network>
			<map name="test-map">
				<map-store enabled="true">
					<class-name>com.hazelcast.client.test.SampleMapStore</class-name>
				</map-store>
			</map>
			<serialization>
				<data-serializable-factories>
					<data-serializable-factory factory-id="66">com.hazelcast.client.test.IdentifiedFactory</data-serializable-factory>
					<data-serializable-factory factory-id="666">com.hazelcast.client.test.IdentifiedDataSerializableFactory</data-serializable-factory>
				</data-serializable-factories>
			</serialization>
		</hazelcast>
			`, clusterName, port, mu)
}
