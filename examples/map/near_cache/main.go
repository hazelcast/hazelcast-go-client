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
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

func main() {
	const mapName = "mymap-go"
	const keyCount = 300
	repeat := 1_000
	nearCachEnabled := true
	// load the near cache keys beforehand or not
	warmup := false
	parallel := true

	ctx := context.Background()

	config := hazelcast.Config{}
	config.Logger.Level = logger.InfoLevel
	config.Cluster.Network.SetAddresses("localhost:5701")
	config.Stats.Enabled = true

	if nearCachEnabled {
		ec := nearcache.EvictionConfig{}
		ec.SetEvictionPolicy(nearcache.EvictionPolicyLFU)
		ec.SetSize(keyCount)
		ncc := nearcache.Config{
			Name:     mapName,
			Eviction: ec,
		}
		ncc.SetInvalidateOnChange(true)
		config.AddNearCache(ncc)
	}

	cl, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}
	m, err := cl.GetMap(context.Background(), mapName)

	for i := 0; i < keyCount; i++ {
		value := fmt.Sprintf("value-%d", i)
		if err := m.Set(ctx, i, value); err != nil {
			panic(err)
		}
	}

	if warmup {
		for i := 0; i < keyCount; i++ {
			if _, err := m.Get(ctx, i); err != nil {
				panic(err)
			}
		}
		repeat -= 1
	}

	wg := &sync.WaitGroup{}
	if parallel {
		wg.Add(keyCount * repeat)
	}
	tic := time.Now()

	for i := 0; i < repeat; i++ {
		if i%1000 == 0 {
			fmt.Println("Iteration", i+1)
		}
		if parallel {
			doParallel(keyCount, m, wg)
		} else {
			doSequential(keyCount, m)
		}
	}
	wg.Wait()

	toc := time.Now()
	fmt.Println("Took:", toc.Sub(tic))
	stats := m.LocalMapStats().NearCacheStats
	fmt.Printf("hits: %d, misses: %d, owned: %d\n", stats.Hits, stats.Misses, stats.OwnedEntryCount)

	if err := m.Destroy(ctx); err != nil {
		panic(err)
	}
	cl.Shutdown(context.Background())
	fmt.Println("shutdown")
}

func doSequential(keyCount int, m *hazelcast.Map) {
	for i := 0; i < keyCount; i++ {
		if _, err := m.Get(context.Background(), i); err != nil {
			panic(err)
		}
	}

}

func doParallel(keyCount int, m *hazelcast.Map, wg *sync.WaitGroup) {
	for i := 0; i < keyCount; i++ {
		go func(i int) {
			if _, err := m.Get(context.Background(), i); err != nil {
				panic(err)
			}
			wg.Done()
		}(i)
	}
}
