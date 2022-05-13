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
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func main() {
	ctx := context.TODO()
	// Start the client with defaults.
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(err)
	}
	// Get a reference to the map.
	myMap, err := client.GetMap(ctx, "my-map")
	if err != nil {
		panic(err)
	}
	// Populate the map, error handling is omitted here for brevity
	_ = myMap.Set(ctx, "key-1", serialization.JSON(`{"property: 5}`))
	_ = myMap.Set(ctx, "key-2", serialization.JSON(`{"property": 10}`))
	_ = myMap.Set(ctx, "key-3", serialization.JSON(`{"property": 15}`))
	// Filter the entries in the map based on a predicate and print those.
	pred := predicate.And(predicate.Greater("property", 8), predicate.Less("property", 12))
	entries, err := myMap.GetEntrySetWithPredicate(ctx, pred)
	if err != nil {
		panic(err)
	}
	fmt.Println(entries)
}
