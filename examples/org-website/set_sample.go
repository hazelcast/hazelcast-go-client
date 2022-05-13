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
	// Get a reference to the set.
	mySet, err := client.GetSet(ctx, "my-set")
	if err != nil {
		panic(err)
	}
	// Add items to set.
	// Note that value1 is added twice.
	changed, err := mySet.AddAll(ctx, "value1", "value2", "value1", "value3")
	if err != nil {
		panic(err)
	}
	fmt.Println("Was the set changed?", changed)
	// Print contents of the set
	items, err := mySet.GetAll(ctx)
	if err != nil {
		panic(err)
	}
	// Get the items back and print them.
	fmt.Println("Got the items back:", items)
}
