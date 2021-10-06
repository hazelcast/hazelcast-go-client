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
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random replicated map
	rand.Seed(time.Now().Unix())
	replicatedMapName := fmt.Sprintf("sample-%d", rand.Int())
	replicatedMap, err := client.GetReplicatedMap(ctx, replicatedMapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate map
	replacedValue, err := replicatedMap.Put(ctx, "key", "value")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(replacedValue)
	// Get value and print
	value, err := replicatedMap.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
	// Shutdown client
	client.Shutdown(ctx)
}
