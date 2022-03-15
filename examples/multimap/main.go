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
)

func main() {
	// Start the client with defaults.
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random map.
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := client.GetMultiMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate the map.
	success, err := m.Put(ctx, "key", "value1")
	if err != nil {
		log.Fatal(err)
	}
	if !success {
		log.Fatal("multi-map put operation failed")
	}
	success, err = m.Put(ctx, "key", "value2")
	if err != nil {
		log.Fatal(err)
	}
	if !success {
		log.Fatal("multi-map put operation failed")
	}
	// Get both values under the same key.
	values, err := m.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}
	// An interface slice contains the values.
	// []interface {}{"value2", "value1"}
	fmt.Printf("%#v", values)
	// Check existence of an entry
	ok, err := m.ContainsEntry(ctx, "key", "value1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("entry found:%t\n", ok)
	// Print number of values that match the given "key"
	count, err := m.ValueCount(ctx, "key")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Number of values for %s: %v", "key", count)
}
