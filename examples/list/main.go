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
	// Get a random list
	rand.Seed(time.Now().Unix())
	listName := fmt.Sprintf("sample-%d", rand.Int())
	list, err := client.GetList(ctx, listName)
	if err != nil {
		log.Fatal(err)
	}
	// Get and print list size
	size, err := list.Size(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(size)
	// Add data, error handling is omitted here for brevity
	list.Add(ctx, "Item 1")
	list.Add(ctx, "Item 2")
	// Get and print list size
	size, err = list.Size(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(size)
	// Shutdown client
	client.Shutdown(ctx)
}
