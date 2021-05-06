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
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

func ReplicatedMapTesterWithConfigBuilder(t *testing.T, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.ReplicatedMap)) {
	makeMapName := func() string {
		return fmt.Sprintf("test-replicated-map-%d-%d", idGen.NextID(), rand.Int())
	}
	ReplicatedMapTesterWithConfigBuilderWithName(t, makeMapName, cbCallback, f)
}

func ReplicatedMapTesterWithConfigBuilderWithName(t *testing.T, makeMapName func() string, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, m *hz.ReplicatedMap)) {
	var (
		client *hz.Client
		m      *hz.ReplicatedMap
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cb := defaultTestCluster.DefaultConfigBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Cluster().SetSmartRouting(smart)
		client, m = getClientReplicatedMapWithConfig(makeMapName(), cb)
		defer func() {
			m.Clear()
			client.Shutdown()
		}()
		f(t, m)
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

func getClientReplicatedMapWithConfig(name string, cb *hz.ConfigBuilder) (*hz.Client, *hz.ReplicatedMap) {
	if TraceLoggingEnabled() {
		cb.Logger().SetLevel(logger.TraceLevel)
	} else {
		cb.Logger().SetLevel(logger.WarnLevel)
	}
	client, err := hz.StartNewClientWithConfig(cb)
	if err != nil {
		panic(err)
	}
	if m, err := client.GetReplicatedMap(name); err != nil {
		panic(err)
	} else {
		return client, m
	}
}
