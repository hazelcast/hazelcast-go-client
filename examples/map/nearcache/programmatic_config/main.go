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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

func printStats(m *hazelcast.Map) {
	stats := m.LocalMapStats().NearCacheStats
	fmt.Printf("hits: %d, misses: %d, owned: %d\n", stats.Hits, stats.Misses, stats.OwnedEntryCount)
}

func main() {
	const mapName = "mymap-go"
	const keyCount = 1_000
	repeat := 1_000
	ctx := context.Background()
	// configuration
	config := hazelcast.Config{}
	config.Stats.Enabled = true
	ec := nearcache.EvictionConfig{}
	ec.SetPolicy(nearcache.EvictionPolicyLFU)
	ec.SetSize(keyCount)
	ncc := nearcache.Config{
		Name:     mapName,
		Eviction: ec,
	}
	ncc.SetInvalidateOnChange(true)
	config.AddNearCache(ncc)
	// setup
	cl, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	m, err := cl.GetMap(context.Background(), mapName)
	// populate the map
	for i := 0; i < keyCount; i++ {
		value := fmt.Sprintf("value-%d", i)
		if err := m.Set(ctx, i, value); err != nil {
			panic(err)
		}
	}
	printStats(m)
	// start hitting the near cache
	tic := time.Now()
	for i := 1; i <= repeat; i++ {
		if i%1000 == 0 {
			fmt.Println("Iteration", i)
		}
		for i := 0; i < keyCount; i++ {
			if _, err := m.Get(context.Background(), i); err != nil {
				panic(err)
			}
		}
	}
	toc := time.Now()
	// done
	fmt.Println("Took:", toc.Sub(tic))
	printStats(m)
}
