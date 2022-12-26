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

package it

import (
	"context"
	"fmt"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func SerializationTester(t *testing.T, f func(t *testing.T, config hazelcast.Config, clusterID, mapName string)) {
	ensureRemoteController(true)
	mapName := NewUniqueObjectName("map")
	f(t, defaultTestCluster.DefaultConfig(), defaultTestCluster.ClusterID, mapName)
}

func inMemoryFormatConfig(clusterName, mapName, inMemoryFormat string, port int32) string {
	return fmt.Sprintf(`
        <hazelcast xmlns="http://www.hazelcast.com/schema/config"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.hazelcast.com/schema/config
            http://www.hazelcast.com/schema/config/hazelcast-config-4.0.xsd">
            <cluster-name>%s</cluster-name>
            <map name="%s">
				<in-memory-format>%s</in-memory-format>
			</map>
			<network>
               <port>%d</port>
            </network>
        </hazelcast>
	`, clusterName, mapName, inMemoryFormat, port)
}

func CompactFormatTester(t *testing.T, mapName string, compactSerializers []serialization.CompactSerializer, port int, f func(t *testing.T, client1, client2 *hazelcast.Client)) {
	ensureRemoteController(false)
	testCases := []struct {
		name           string
		inMemoryFormat string
	}{
		{name: "inMemoryFormat:BINARY", inMemoryFormat: "BINARY"},
		//todo: uncomment this after metadata distribution is implemented {name: "inMemoryFormat:OBJECT", inMemoryFormat: "OBJECT"},
	}
	for _, tc := range testCases {
		testName := t.Name() + tc.name
		t.Run(testName, func(t *testing.T) {
			tc := StartNewClusterWithConfig(2, inMemoryFormatConfig(testName, mapName, tc.inMemoryFormat, int32(port)), port)
			defer tc.Shutdown()
			config := tc.DefaultConfig()
			config.Serialization.Compact.SetSerializers(compactSerializers...)
			ctx := context.Background()
			client := MustClient(hazelcast.StartNewClientWithConfig(ctx, config))
			client2 := MustClient(hazelcast.StartNewClientWithConfig(ctx, config))
			defer client.Shutdown(ctx)
			defer client2.Shutdown(ctx)
			f(t, client, client2)
		})
	}
}
