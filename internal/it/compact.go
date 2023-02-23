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

package it

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func MakeCompactClusterConfig(clusterName, mapName, inMemoryFormat string, port int) string {
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

type CompactTestContext struct {
	T            *testing.T
	M            *hazelcast.Map
	Cluster      *TestCluster
	Client1      *hazelcast.Client
	Client2      *hazelcast.Client
	MapName      string
	InMemFmt     string
	Serializers1 []serialization.CompactSerializer
	Serializers2 []serialization.CompactSerializer
}

func (tcx CompactTestContext) Tester(f func(CompactTestContext)) {
	ensureRemoteController(true)
	runner := func(tcx CompactTestContext) {
		if tcx.MapName == "" {
			tcx.MapName = NewUniqueObjectName("map")
		}
		if tcx.Cluster == nil {
			cn := fmt.Sprintf("%s-%s", tcx.T.Name(), tcx.InMemFmt)
			port := NextPort()
			cfg := MakeCompactClusterConfig(cn, tcx.MapName, tcx.InMemFmt, port)
			tcx.Cluster = StartNewClusterWithConfig(2, cfg, port)
		}
		cfg1 := tcx.Cluster.DefaultConfig()
		cfg1.Serialization.Compact.SetSerializers(tcx.Serializers1...)
		tcx.T.Logf("map name: %s", tcx.MapName)
		tcx.T.Logf("cluster address: %s", cfg1.Cluster.Network.Addresses[0])
		if tcx.Client1 == nil {
			tcx.Client1 = getDefaultClient(&cfg1)
		}
		cfg2 := tcx.Cluster.DefaultConfig()
		cfg2.Serialization.Compact.SetSerializers(tcx.Serializers1...)
		if tcx.Client2 == nil {
			tcx.Client2 = getDefaultClient(&cfg2)
		}
		if tcx.M == nil {
			m, err := tcx.Client1.GetMap(context.Background(), tcx.MapName)
			if err != nil {
				panic(err)
			}
			tcx.M = m
		}
		defer func() {
			ctx := context.Background()
			if err := tcx.M.Destroy(ctx); err != nil {
				tcx.T.Logf("test warning, could not destroy map: %s", err.Error())
			}
			if err := tcx.Client1.Shutdown(ctx); err != nil {
				tcx.T.Logf("Test warning, client-1 not shutdown: %s", err.Error())
			}
			if err := tcx.Client2.Shutdown(ctx); err != nil {
				tcx.T.Logf("Test warning, client-2 not shutdown: %s", err.Error())
			}
			tcx.Cluster.Shutdown()
		}()
		f(tcx)
	}
	for _, mf := range []string{"BINARY", "OBJECT"} {
		tcx.T.Run(mf, func(t *testing.T) {
			tt := tcx
			tt.InMemFmt = mf
			tt.T = t
			runner(tt)
		})
	}
}

func (tcx CompactTestContext) ExecuteScript(ctx context.Context, script string) error {
	clusterID := tcx.Cluster.ClusterID
	r, err := tcx.Cluster.RC.ExecuteOnController(ctx, clusterID, script, Lang_JAVASCRIPT)
	if err != nil {
		return err
	}
	if !r.Success {
		return errors.New(r.Message)
	}
	return nil
}
