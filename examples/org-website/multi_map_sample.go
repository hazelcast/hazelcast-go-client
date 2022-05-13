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
	// Get a reference to the multi map.
	myMap, err := client.GetMultiMap(ctx, "my-multi-map")
	if err != nil {
		panic(err)
	}
	// Put values in the map with the same key.
	err = myMap.PutAll(ctx, "some-key", "some-value-1", "some-value-2", "some-value-3")
	if err != nil {
		panic(err)
	}
	// Get the values back and print it.
	values, err := myMap.Get(ctx, "some-key")
	if err != nil {
		panic(err)
	}
	fmt.Println("Got the values back:", values)
}
