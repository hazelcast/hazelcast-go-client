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
	"os"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func bye(format string, args ...interface{}) {
	// ignoring the error
	_, _ = fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func printStats(m *hazelcast.Map) {
	stats := m.LocalMapStats().NearCacheStats
	fmt.Printf("hits: %d, misses: %d, owned: %d\n", stats.Hits, stats.Misses, stats.OwnedEntryCount)
}

func measure(f func()) time.Duration {
	tic := time.Now()
	f()
	toc := time.Now()
	return toc.Sub(tic)
}

func main() {
	// startup
	if len(os.Args) != 2 {
		bye("Usage: %s config.json", os.Args[0])
	}
	var config Config
	if err := config.LoadFromPath(os.Args[1]); err != nil {
		bye(err.Error())
	}
	ctx := context.Background()
	cl, err := hazelcast.StartNewClientWithConfig(ctx, config.Client)
	if err != nil {
		panic(err)
	}
	m, err := cl.GetMap(context.Background(), config.MapName)
	if err != nil {
		panic(err)
	}
	// populate the map
	for i := int64(0); i < int64(config.KeyCount); i++ {
		value := fmt.Sprintf("value-%d", i)
		if err := m.Set(ctx, i, value); err != nil {
			panic(err)
		}
	}
	// warmup
	if config.Warmup {
		for i := int64(0); i < int64(config.KeyCount); i++ {
			if _, err := m.Get(ctx, i); err != nil {
				panic(err)
			}
		}
	}
	// run
	runner := SimpleRunner{}
	printStats(m)
	var opCnt int64
	took := measure(func() {
		for i := 1; i <= config.Repeat; i++ {
			if i%1000 == 0 {
				fmt.Println("Iteration", i)
			}
			opCnt += runner.Run(config, m)
		}
	})
	fmt.Printf("Took: %v, Op Count: %d\n", took, opCnt)
	printStats(m)
}
