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
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
)

func ReplicatedMapTester(t *testing.T, f func(t *testing.T, m *hz.ReplicatedMap)) {
	ReplicatedMapTesterWithConfig(t, nil, f)
}

func ReplicatedMapTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, m *hz.ReplicatedMap)) {
	makeMapName := func() string {
		return NewUniqueObjectName("replicated-map")
	}
	ReplicatedMapTesterWithConfigAndName(t, makeMapName, configCallback, f)
}

func ReplicatedMapTesterWithConfigAndName(t *testing.T, makeMapName func() string, cbCallback func(cb *hz.Config), f func(t *testing.T, m *hz.ReplicatedMap)) {
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
		cls := defaultTestCluster.Launch(t)
		config := cls.DefaultConfig()
		if cbCallback != nil {
			cbCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, m = getClientReplicatedMapWithConfig(makeMapName(), &config)
		defer func() {
			ctx := context.Background()
			if err := m.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy replicated map: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("test warning, client not shutdown: %s", err.Error())
			}
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

func getClientReplicatedMapWithConfig(name string, config *hz.Config) (*hz.Client, *hz.ReplicatedMap) {
	client := getDefaultClient(config)
	if m, err := client.GetReplicatedMap(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, m
	}
}
