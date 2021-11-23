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
	"reflect"
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
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate the map.
	replacedValue, err := m.Put(ctx, "key", "value")
	if err != nil {
		log.Fatal(err)
	}
	// Get and print result.
	fmt.Println(replacedValue)
	value, err := m.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
	// Atomic putIfAbsent method.
	val, err := m.PutIfAbsent(ctx, "key-2", "value-2")
	if err != nil {
		log.Fatal(err)
	}
	if isNil(val) {
		fmt.Println("Inserted value")
	}
	// Atomic replace if same method.
	replaced, err := m.ReplaceIfSame(ctx, "key-2", "value-2", "value3")
	if err != nil {
		log.Fatal(err)
	}
	if replaced {
		fmt.Println("Replaced value")
	}
	// Shutdown client.
	client.Shutdown(ctx)
}

func isNil(arg interface{}) bool {
	if arg == nil {
		return true
	}
	value := reflect.ValueOf(arg)
	return value.Kind() == reflect.Ptr && value.IsNil()
}
