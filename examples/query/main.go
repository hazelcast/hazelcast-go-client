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

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func main() {
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random map
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate the map, error handling is omitted for brevity
	m.Put(ctx, "key-1", serialization.JSON(`{"property: 5}`))
	m.Put(ctx, "key-2", serialization.JSON(`{"property": 10}`))
	m.Put(ctx, "key-3", serialization.JSON(`{"property": 15}`))
	// Print all entries in the map
	entries, err := m.GetEntrySet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(entries)
	// Filter the entries in the map based on a predicate and print those
	pred := predicate.And(predicate.Less("property", 12), predicate.Greater("property", 8))
	entries, err = m.GetEntrySetWithPredicate(ctx, pred)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(entries)
	// Shutdown client
	client.Shutdown(ctx)
}
