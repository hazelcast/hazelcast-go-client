//go:build ignore
// +build ignore

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

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	ctx := context.TODO()
	// Start the client with defaults.
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(err)
	}
	// Get a reference to the replicated map.
	myMap, err := client.GetReplicatedMap(ctx, "my-replicated-map")
	if err != nil {
		panic(err)
	}
	// Put a value in the map and retrieve the previous value.
	oldValue, err := myMap.Put(ctx, "some-key", "some-value")
	if err != nil {
		panic(err)
	}
	fmt.Println("Previous value:", oldValue)
	// Get the value back and print it.
	value, err := myMap.Get(ctx, "some-key")
	if err != nil {
		panic(err)
	}
	fmt.Println("Got the value back:", value)
}
