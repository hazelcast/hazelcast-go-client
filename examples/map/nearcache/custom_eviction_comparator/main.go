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
	"math/rand"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

type CustomEvictionComparator struct{}

func (c CustomEvictionComparator) Compare(a, b nearcache.EvictableEntryView) int {
	k1 := a.Key().(string)
	k2 := b.Key().(string)
	if k1 < k2 {
		return -1
	}
	if k1 > k2 {
		return 1
	}
	return 0
}

func makeKey() string {
	letters := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(letters), func(i, j int) {
		letters[i], letters[j] = letters[j], letters[i]
	})
	key := letters[:10]
	return fmt.Sprintf("%s-%d", key, rand.Intn(1000))
}

func main() {
	const mapName = "mymap1"
	const keyCount = 100
	ec := nearcache.EvictionConfig{}
	ec.SetComparator(&CustomEvictionComparator{})
	ec.SetSize(keyCount / 10)
	ncc := nearcache.Config{
		Name:     mapName,
		Eviction: ec,
	}
	config := hazelcast.Config{}
	config.AddNearCache(ncc)
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		panic(fmt.Errorf("starting the client: %w", err))
	}
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		panic(fmt.Errorf("getting the map: %w", err))
	}
	// populate the map
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		key := makeKey()
		fmt.Println(key)
		if err := m.Set(ctx, key, i); err != nil {
			panic(fmt.Errorf("setting on the map: %w", err))
		}
		keys[i] = key
	}
	// populate the near cache
	for _, key := range keys {
		v, err := m.Get(ctx, key)
		if err != nil {
			panic(fmt.Errorf("getting from the map: %w", err))
		}
		fmt.Println("Got", v)
	}
}
