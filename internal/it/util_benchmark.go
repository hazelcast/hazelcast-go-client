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
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/logger"

	hz "github.com/hazelcast/hazelcast-go-client"
)

const EnvWarmupCount = "WARMUPS"

func Benchmarker(b *testing.B, f func(b *testing.B, config *hz.Config)) {
	BenchmarkerWithConfigBuilder(b, nil, f)
}

func BenchmarkerWithConfigBuilder(b *testing.B, configCallback func(*hz.Config), f func(b *testing.B, config *hz.Config)) {
	ensureRemoteController(true)
	runner := func(b *testing.B, smart bool) {
		cls := defaultTestCluster.Launch(b)
		config := cls.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Logger.Level = logger.ErrorLevel
		config.Cluster.Unisocket = !smart
		b.ResetTimer()
		f(b, &config)
	}
	if SmartEnabled() {
		b.Run("Smart Client", func(b *testing.B) {
			runner(b, true)
		})
	}
	if NonSmartEnabled() {
		b.Run("Non-Smart Client", func(b *testing.B) {
			runner(b, false)
		})
	}
}

func MapBenchmarker(t *testing.B, fixture func(m *hz.Map), f func(t *testing.B, m *hz.Map)) {
	configCallback := func(cb *hz.Config) {
		cb.Logger.Level = logger.WarnLevel
	}
	MapBenchmarkerWithConfigBuilder(t, configCallback, fixture, f)
}

func MapBenchmarkerWithConfigBuilder(t *testing.B, cbCallback func(*hz.Config), fixture func(m *hz.Map), f func(t *testing.B, m *hz.Map)) {
	makeMapName := func() string {
		return fmt.Sprintf("bm-map-%d", rand.Int())
	}
	MapBenchmarkerWithConfigAndName(t, makeMapName, cbCallback, fixture, f)
}

func MapBenchmarkerWithConfigAndName(b *testing.B, makeMapName func() string, cbCallback func(config *hz.Config), fixture func(m *hz.Map), f func(b *testing.B, m *hz.Map)) {
	var (
		client *hz.Client
		m      *hz.Map
	)
	ensureRemoteController(true)
	warmups := warmupCount()
	if warmups > 0 {
		b.Logf("Warmups: %d", warmups)
	}
	runner := func(b *testing.B, smart bool) {
		client, m = getMap(b, makeMapName(), cbCallback, smart)
		defer func() {
			ctx := context.Background()
			m.EvictAll(ctx)
			if err := client.Shutdown(ctx); err != nil {
				b.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		if fixture != nil {
			fixture(m)
		}
		b.ResetTimer()
		f(b, m)
	}
	warmJvmUp := func() {
		client, m := getMap(b, makeMapName(), cbCallback, true)
		for i := 0; i < warmups; i++ {
			f(b, m)
		}
		ctx := context.Background()
		m.EvictAll(ctx)
		client.Shutdown(ctx)
	}
	if SmartEnabled() {
		b.Run("Smart Client", func(b *testing.B) {
			warmJvmUp()
			runner(b, true)
		})
	}
	if NonSmartEnabled() {
		b.Run("Non-Smart Client", func(b *testing.B) {
			warmJvmUp()
			runner(b, false)
		})
	}
}

func warmupCount() int {
	if s := os.Getenv(EnvWarmupCount); s != "" {
		if i, err := strconv.ParseInt(s, 10, 32); err != nil {
			panic(err)
		} else {
			return int(i)
		}
	}
	return 3
}

func getMap(b *testing.B, mapName string, configCallback func(*hz.Config), smart bool) (*hz.Client, *hz.Map) {
	cls := defaultTestCluster.Launch(b)
	config := cls.DefaultConfig()
	if configCallback != nil {
		configCallback(&config)
	}
	config.Cluster.Unisocket = !smart
	return GetClientMapWithConfig(mapName, &config)
}
