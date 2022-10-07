/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package it

import (
	"context"
	"fmt"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

var sqlTestCluster = NewSingletonTestCluster("sql", func() *TestCluster {
	const clusterName = "sql-integration-test"
	port := NextPort()
	memberConfig := SQLXMLConfig(clusterName, "localhost", port)
	if SSLEnabled() {
		memberConfig = sqlXMLSSLConfig(clusterName, "localhost", port)
	}
	return StartNewClusterWithConfig(MemberCount(), memberConfig, port)
})

func SQLTester(t *testing.T, f func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string)) {
	cfn := func(c *hz.Config) {
		c.Serialization.SetGlobalSerializer(&PanicingGlobalSerializer{})
	}
	SQLTesterWithConfigBuilder(t, cfn, f)
}

func SQLTesterWithConfigBuilder(t *testing.T, configFn func(config *hz.Config), f func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string)) {
	tc := sqlTestCluster.Launch(t)
	runner := func(t *testing.T, smart bool) {
		config := tc.DefaultConfig()
		if configFn != nil {
			configFn(&config)
		}
		config.Cluster.Unisocket = !smart
		ls := "smart"
		if !smart {
			ls = "unisocket"
		}
		mapName := NewUniqueObjectName("map", ls)
		client, m := GetClientMapWithConfig(mapName, &config)
		defer func() {
			ctx := context.Background()
			if err := m.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy map: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, client, &config, m, mapName)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}

func SQLXMLConfig(clusterName, publicAddr string, port int) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <network>
				<public-address>%s</public-address>
				<port>%d</port>
            </network>
			<serialization>
				<portable-factories>
					<portable-factory factory-id="666">com.hazelcast.client.test.PortableFactory
					</portable-factory>
				</portable-factories>
			</serialization>
			<jet enabled="true" />
        </hazelcast>
	`, clusterName, publicAddr, port)
}

func sqlXMLSSLConfig(clusterName, publicAddr string, port int) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <network>
				<public-address>%s</public-address>
				<port>%d</port>
				<ssl enabled="true">
					<factory-class-name>
						com.hazelcast.nio.ssl.ClasspathSSLContextFactory
					</factory-class-name>
					<properties>
						<property name="keyStore">com/hazelcast/nio/ssl-mutual-auth/server1.keystore</property>
						<property name="keyStorePassword">password</property>
						<property name="keyManagerAlgorithm">SunX509</property>
						<property name="protocol">TLSv1.2</property>
					</properties>
				</ssl>
            </network>
			<serialization>
				<portable-factories>
					<portable-factory factory-id="666">com.hazelcast.client.test.PortableFactory
					</portable-factory>
				</portable-factories>
			</serialization>
			<jet enabled="true" />
        </hazelcast>
	`, clusterName, publicAddr, port)
}

type PanicingGlobalSerializer struct{}

func (p PanicingGlobalSerializer) ID() (id int32) {
	return 1000
}

func (p PanicingGlobalSerializer) Read(input serialization.DataInput) interface{} {
	panic("panicing global serializer: read")
}

func (p PanicingGlobalSerializer) Write(output serialization.DataOutput, object interface{}) {
	panic("panicing global serializer: write")
}
