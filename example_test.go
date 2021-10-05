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
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

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
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve the set named my-set.
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
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve the PN counter named my-pn.
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
}

func ExampleMap() {
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
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

func ExampleMap_Aggregate() {
	ctx := context.TODO()
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
}

func ExampleMap_AddEntryListener() {
	// error handling was omitted for brevity
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	entryListenerConfig := hazelcast.MapEntryListenerConfig{
		IncludeValue: true,
	}
	m, err := client.GetMap(ctx, "somemap")
	if err != nil {
		log.Fatal(err)
	}
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
		log.Fatal(err)
	}
	// performing modifications on the map entries
	key := strconv.Itoa(int(time.Now().Unix()))
	if err := m.Set(ctx, key, "1"); err != nil {
		log.Fatal(err)
	}
	if err := m.Set(ctx, key, "2"); err != nil {
		log.Fatal(err)
	}
	if err := m.Delete(ctx, key); err != nil {
		log.Fatal(err)
	}
	// you can use the subscriptionID later to remove the event listener.
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		log.Fatal(err)
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
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random map.
	myMap, err := client.GetMap(ctx, "map")
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

func ExampleQueue() {
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random queue
	q, err := client.GetQueue(ctx, "queue-1")
	if err != nil {
		log.Fatal(err)
	}
	// Add an item to the queue if space is available (non-blocking)
	added, err := q.Add(ctx, "item 1")
	if err != nil {
		log.Fatal(err)
	}
	if added {
		fmt.Println("Added item 1")
	}
	// Get the head of the queue if available and print item
	item, err := q.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(item)
	// Add an item waiting for capacity until timeout
	added, err = q.AddWithTimeout(ctx, "item 2", 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if added {
		fmt.Println("Added item 2")
	}
	// Wait indefinetely to add an item
	err = q.Put(ctx, "item 3")
	if err != nil {
		log.Fatal(err)
	}
	// Wait indefintely to take the head and print item
	item, err = q.Take(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(item)
	// Shutdown client
	client.Shutdown(ctx)
}

func ExampleReplicatedMap() {
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random replicated map
	replicatedMap, err := client.GetReplicatedMap(ctx, "replicated-map-1")
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

func ExampleTopic() {
	// messageListener handles incoming messages to the topic
	messageListener := func(event *hazelcast.MessagePublished) {
		fmt.Println("Received message: ", event.Value)
	}
	messageCount := 10
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random topic
	rand.Seed(time.Now().Unix())
	topicName := fmt.Sprintf("sample-%d", rand.Int())
	topic, err := client.GetTopic(ctx, topicName)
	if err != nil {
		log.Fatal(err)
	}
	// Add a message listener to the topic
	topic.AddMessageListener(ctx, messageListener)
	// Publish messages to topic
	for i := 0; i < messageCount; i++ {
		topic.Publish(ctx, fmt.Sprintf("Message %d", i))
	}
	// Shutdown client
	client.Shutdown(ctx)
}

func ExampleMap_GetEntrySetWithPredicate() {
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random map
	m, err := client.GetMap(ctx, "map-1")
	if err != nil {
		log.Fatal(err)
	}
	// Populate the map, error handling is omitted here for brevity
	m.Put(ctx, "key-1", serialization.JSON(`{"property: 5}`))
	m.Put(ctx, "key-2", serialization.JSON(`{"property": 10}`))
	m.Put(ctx, "key-3", serialization.JSON(`{"property": 15}`))
	// Filter the entries in the map based on a predicate and print those
	pred := predicate.And(predicate.Less("property", 12), predicate.Greater("property", 8))
	entries, err := m.GetEntrySetWithPredicate(ctx, pred)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(entries)
	// Shutdown client
	client.Shutdown(ctx)
}

func ExampleList() {
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random list
	list, err := client.GetList(ctx, "list-1")
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
