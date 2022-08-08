package hazelcast_test

import (
	"context"
	"fmt"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

type MutualAuthenticationStatus string

const (
	OPTIONAL MutualAuthenticationStatus = "OPTIONAL"
	REQUIRED MutualAuthenticationStatus = "REQUIRED"
)

func Test_MutualAuthenticationRequired(t *testing.T) {
	it.MarkFlaky(t, "pkcs12 fails while decrypting")
	ctx := context.Background()
	port := it.NextPort()
	clsConfig := xmlSSLMutualAuthenticationConfig(t.Name(), port, REQUIRED)
	tc := it.StartNewClusterWithConfig(1, clsConfig, port)
	defer tc.Shutdown()
	clientCfg := tc.DefaultConfig()
	clientCfg.Cluster.Network.SSL = cluster.SSLConfig{Enabled: true}
	sslConfig := &clientCfg.Cluster.Network.SSL
	tls := sslConfig.TLSConfig()
	tls.InsecureSkipVerify = true
	sslConfig.SetTLSConfig(tls)
	if err := sslConfig.SetCAPath("cluster/testdata/OpenSSL/rootCA.crt"); err != nil {
		t.Error(err)
	}
	if err := sslConfig.AddClientCertAndEncryptedKeyPath("cluster/testdata/OpenSSL/client/client.crt",
		"cluster/testdata/OpenSSL/client/client.key", "hazelcast"); err != nil {
		t.Fatal(err)
	}
	client, err := hz.StartNewClientWithConfig(ctx, clientCfg)
	if err != nil {
		t.Error(err)
	}
	if err = client.Shutdown(ctx); err != nil {
		t.Error(err)
	}
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
						<property name="keyStore">cluster/testdata/OpenSSL/server.keystore</property>
						<property name="keyStorePassword">hazelcast</property>
						<property name="keyStore">cluster/testdata/OpenSSL/server.truststore</property>
						<property name="trustStorePassword">hazelcast</property>
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
