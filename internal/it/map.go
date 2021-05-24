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
	"math/rand"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"go.uber.org/goleak"
)

func MapTester(t *testing.T, f func(t *testing.T, m *hz.Map)) {
	MapTesterWithConfig(t, nil, f)
}

func MapTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, m *hz.Map)) {
	makeMapName := func() string {
		return fmt.Sprintf("test-map-%d-%d", idGen.NextID(), rand.Int())
	}
	MapTesterWithConfigAndName(t, makeMapName, configCallback, f)
}

func MapTesterWithConfigAndName(t *testing.T, makeMapName func() string, configCallback func(*hz.Config), f func(t *testing.T, m *hz.Map)) {
	var (
		client *hz.Client
		m      *hz.Map
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		config := defaultTestCluster.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.ClusterConfig.SmartRouting = smart
		client, m = GetClientMapWithConfig(makeMapName(), &config)
		defer func() {
			if err := m.Destroy(); err != nil {
				t.Logf("test warning, could not destroy map: %s", err.Error())
			}
			if err := client.Shutdown(); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
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

func ContextMapTester(t *testing.T, f func(t *testing.T, m *hz.ContextMap)) {
	configCallback := func(cb *hz.Config) {
	}
	ContextMapTesterWithConfigBuilder(t, configCallback, f)
}

func ContextMapTesterWithConfigBuilder(t *testing.T, cbCallback func(*hz.Config), f func(t *testing.T, m *hz.ContextMap)) {
	makeMapName := func() string {
		return fmt.Sprintf("test-context-map-%d-%d", idGen.NextID(), rand.Int())
	}
	ContextMapTesterWithConfigAndName(t, makeMapName, cbCallback, f)
}

func ContextMapTesterWithConfigAndName(t *testing.T, makeMapName func() string, cbCallback func(*hz.Config), f func(t *testing.T, m *hz.ContextMap)) {
	var (
		client *hz.Client
		m      *hz.ContextMap
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		config := defaultTestCluster.DefaultConfig()
		if cbCallback != nil {
			cbCallback(&config)
		}
		config.ClusterConfig.SmartRouting = smart
		client, m = GetClientContextMapWithConfig(makeMapName(), &config)
		defer func() {
			m.EvictAll(context.Background())
			if err := client.Shutdown(); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
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

func GetClientMapWithConfig(mapName string, config *hz.Config) (*hz.Client, *hz.Map) {
	client := getDefaultClient(config)
	if m, err := client.GetMap(mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}

func GetClientContextMapWithConfig(mapName string, cb *hz.Config) (*hz.Client, *hz.ContextMap) {
	client := getDefaultClient(cb)
	if m, err := client.GetMapWithContext(mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}
