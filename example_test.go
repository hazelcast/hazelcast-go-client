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
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/aggregate"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

var client = getClient()

func Example() {
	// Create the configuration
	config := hazelcast.Config{}
	config.Cluster.Name = "dev"
	config.Cluster.Network.SetAddresses("localhost:5701")
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
	ctx := context.TODO()
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
	ctx := context.TODO()
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

func ExampleMap_AddEntryListener() {
	// error handling was omitted for brevity
	ctx := context.TODO()
	entryListenerConfig := hazelcast.MapEntryListenerConfig{
		IncludeValue: true,
	}
	m, _ := client.GetMap(ctx, "somemap")
	// enable receiving entry added events
	entryListenerConfig.NotifyEntryAdded(true)
	// enable receiving entry removed events
	entryListenerConfig.NotifyEntryRemoved(true)
	// enable receiving entry updated events
	entryListenerConfig.NotifyEntryUpdated(true)
	// enable receiving entry evicted events
	entryListenerConfig.NotifyEntryEvicted(true)
	// enable receiving entry loaded events
	entryListenerConfig.NotifyEntryLoaded(true)
	subscriptionID, err := m.AddEntryListener(ctx, entryListenerConfig, func(event *hazelcast.EntryNotified) {
		switch event.EventType {
		// this is an entry added event
		case hazelcast.EntryAdded:
			fmt.Println("Entry Added:", event.Value)
		// this is an entry removed event
		case hazelcast.EntryRemoved:
			fmt.Println("Entry Removed:", event.Value)
		// this is an entry updated event
		case hazelcast.EntryUpdated:
			fmt.Println("Entry Updated:", event.Value)
		// this is an entry evicted event
		case hazelcast.EntryEvicted:
			fmt.Println("Entry Remove:", event.Value)
		// this is an entry loaded event
		case hazelcast.EntryLoaded:
			fmt.Println("Entry Loaded:", event.Value)
		}
	})
	if err != nil {
		panic(err)
	}
	// performing modifications on the map entries
	key := strconv.Itoa(int(time.Now().Unix()))
	if err := m.Set(ctx, key, "1"); err != nil {
		panic(err)
	}
	if err := m.Set(ctx, key, "2"); err != nil {
		panic(err)
	}
	if err := m.Delete(ctx, key); err != nil {
		panic(err)
	}
	// you can use the subscriptionID later to remove the event listener.
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		panic(err)
	}
}

func ExampleMap_NewLockContext() {
	// lockAndIncrement locks the given key, reads the value from it and sets back the incremented value.
	lockAndIncrement := func(myMap *hazelcast.Map, key string, wg *sync.WaitGroup) {
		// Signal completion before this goroutine exits.
		defer wg.Done()
		intValue := int64(0)
		// Create a new unique lock context.
		lockCtx := myMap.NewLockContext(context.Background())
		// Lock the key.
		// The key cannot be unlocked without the same lock context.
		if err := myMap.Lock(lockCtx, key); err != nil {
			panic(err)
		}
		// Remember to unlock the key, otherwise it won't be accessible elsewhere.
		defer myMap.Unlock(lockCtx, key)
		// The same lock context, or a derived one from that lock context must be used,
		// otherwise the Get operation below will block.
		v, err := myMap.Get(lockCtx, key)
		if err != nil {
			panic(err)
		}
		// If v is not nil, then there's already a value for the key.
		if v != nil {
			intValue = v.(int64)
		}
		// Increment and set the value back.
		intValue++
		// The same lock context, or a derived one from that lock context must be used,
		// otherwise the Set operation below will block.
		if err = myMap.Set(lockCtx, key, intValue); err != nil {
			panic(err)
		}
	}

	const goroutineCount = 100
	const key = "counter"

	ctx := context.TODO()
	// Get a random map.
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	myMap, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Lock and increment the value stored in key for goroutineCount times.
	wg := &sync.WaitGroup{}
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go lockAndIncrement(myMap, key, wg)
	}
	// Wait for all goroutines to complete.
	wg.Wait()
	// Retrieve the final value.
	// A lock context is not needed, since the key is unlocked.
	if lastValue, err := myMap.Get(context.Background(), key); err != nil {
		panic(err)
	} else {
		fmt.Println("lastValue", lastValue)
	}
	client.Shutdown(ctx)
}

func getClient() *hazelcast.Client {
	var tc = it.StartNewCluster(1)
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), tc.DefaultConfig())
	if err != nil {
		panic(err)
	}
	return client
}
