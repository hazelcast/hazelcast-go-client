// +build noos

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

package hazelcast_test

import (
	"context"
	"fmt"
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/aggregate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func Example() {
	// Create the configuration
	config := hazelcast.Config{}
	config.Cluster.Name = "my-cluster"
	config.Cluster.Network.SetAddresses("192.168.1.42:5000", "192.168.1.42:5001")
	// Start the client with the configuration provider.
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve a map.
	peopleMap, err := client.GetMap(ctx, "people")
	if err != nil {
		log.Fatal(err)
	}
	// Call map functions.
	err = peopleMap.Set(ctx, "jane", "doe")
	if err != nil {
		log.Fatal(err)
	}
	// Stop the client once you are done with it.
	client.Shutdown(ctx)
}

func ExampleSet() {
	// Create the Hazelcast client.
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve the set named my-set
	set, err := client.GetSet(ctx, "my-set")
	if err != nil {
		log.Fatal(err)
	}
	_, err = set.AddAll(ctx, "item1", "item2", "item3", "item2", "item1")
	if err != nil {
		log.Fatal(err)
	}
	// Get the items. Note that there are no duplicates.
	items, err := set.GetAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range items {
		fmt.Println("Item:", item)
	}
}

func ExamplePNCounter() {
	// Create the Hazelcast client.
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve the PN counter named my-pn
	pn, err := client.GetPNCounter(ctx, "my-pn")
	if err != nil {
		log.Fatal(err)
	}
	// Add the given value and retrieve the result.
	_, err = pn.AddAndGet(ctx, 43)
	if err != nil {
		log.Fatal(err)
	}
	// Decrement the given value and retrieve the result.
	value, err := pn.DecrementAndGet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
	// Output: 42
}

func ExampleMap_Aggregate() {
	// Create the Hazelcast client.
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	myMap, err := client.GetMap(ctx, "my-map")
	if err != nil {
		log.Fatal(err)
	}
	if err = myMap.Set(ctx, "k1", serialization.JSON(`{"A": "foo", "B": 10}`)); err != nil {
		log.Fatal(err)
	}
	if err = myMap.Set(ctx, "k2", serialization.JSON(`{"A": "bar", "B": 30}`)); err != nil {
		log.Fatal(err)
	}
	result, err := myMap.Aggregate(ctx, aggregate.LongSum("B"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
	// Output: 40
}
