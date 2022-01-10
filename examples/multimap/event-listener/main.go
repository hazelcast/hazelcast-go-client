/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func ObserveListenerIncludeValueOnly(ctx context.Context, m *hz.MultiMap, myHandler func(*hz.EntryNotified)) {
	fmt.Println("--ObserveListenerIncludeValueOnly: start")
	// In this observation, we will observe EntryAdded, EntryRemoved actions included their value.
	// For this observation, it is needed to register our listener to given MultiMap.
	listenerConfig := hz.MultiMapEntryListenerConfig{IncludeValue: true}
	listenerConfig.NotifyEntryAdded(true)
	listenerConfig.NotifyEntryRemoved(true)
	// Add our continuous entry listener to the given MultiMap.
	subscriptionID, err := m.AddEntryListener(ctx, listenerConfig, myHandler)
	if err != nil {
		panic(err)
	}
	// Initialize entries to put to the given MultiMap.
	myEntries := []types.Entry{
		{Key: "my-key", Value: "my-value1"},
		{Key: "my-key", Value: "my-value2"},
		{Key: "my-key", Value: "my-value3"},
		{Key: "my-another-key", Value: "my-another-value1"},
		{Key: "my-another-key", Value: "my-another-value2"},
	}
	for _, entry := range myEntries {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			panic(err)
		}
	}
	// Then, remove same entries to observe EntryAdded and EntryRemoved actions.
	for _, entry := range myEntries {
		_, err := m.Remove(ctx, entry.Key)
		if err != nil {
			panic(err)
		}
	}

	// If you observe the output, you can clearly see that our handler works fine and notify us for each "my-key" put and remove operation.
	// Also, notice that notification order is not guaranteed to be complied with the order of operation.

	// Remove entry listener from the given MultiMap.
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		panic(err)
	}
	fmt.Println("--ObserveListenerIncludeValueOnly: end")
}

func ObserveListenerOnKey(ctx context.Context, m *hz.MultiMap, myHandler func(*hz.EntryNotified)) {
	fmt.Println("--ObserveListenerOnKey: start")
	// The key that we will listen on later.
	myAwesomeKey := "my-awesome-key"
	// In this observation, we will observe EntryAllCleared action and also EntryAdded, EntryRemoved actions for on a certain key.
	// For this observation, it is needed to register our listener to the given MultiMap.
	listenerConfig := hz.MultiMapEntryListenerConfig{IncludeValue: true, Key: myAwesomeKey}
	listenerConfig.NotifyEntryAllCleared(true)
	listenerConfig.NotifyEntryAdded(true)
	// Add our continuous entry listener to the given MultiMap.
	subscriptionID, err := m.AddEntryListener(ctx, listenerConfig, myHandler)
	if err != nil {
		panic(err)
	}
	// These are the entries to observe that we only interested in the key which we defined above.
	// Events related to dummy key should not be notified and handled.
	myEntries := []types.Entry{
		{Key: myAwesomeKey, Value: "my-awesome-value1"},
		{Key: myAwesomeKey, Value: "my-awesome-value2"},
		{Key: myAwesomeKey, Value: "my-awesome-value3"},
		{Key: "my-dummy-key", Value: "my-dummy-value"},
	}
	// Put my observation entries to the given MultiMap.
	for _, entry := range myEntries {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			panic(err)
		}
	}
	// Trigger a clear event on given MultiMap as well.
	if err := m.Clear(ctx); err != nil {
		panic(err)
	}

	// If you observe the output, you can clearly see that we only handled myAwesomeKey related events then listener ignores "my-dummy-key" related event.
	// Also, notice that notification order is not guaranteed to be complied with the order of operation.

	// Remove entry listener from the given MultiMap.
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		panic(err)
	}
	fmt.Println("--ObserveListenerOnKey: end")
}

func main() {
	ctx := context.TODO()
	// Let's start a new hazelcast client with default config.
	client, err := hz.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Disconnect client from the cluster.
	defer client.Shutdown(ctx)
	// Request an instance of a MultiMap.
	m, err := client.GetMultiMap(ctx, "my-MultiMap")
	if err != nil {
		log.Fatal(err)
	}
	// Define a handler with supported EntryEventType for the upcoming events.
	myHandler := func(event *hz.EntryNotified) {
		switch event.EventType {
		case hz.EntryAdded:
			fmt.Printf("MultiMap: %s, (key: %v, value: %v) was added.\n", event.MapName, event.Key, event.Value)
		case hz.EntryRemoved:
			fmt.Printf("MultiMap: %s, (key: %v, value: %v) was removed.\n", event.MapName, event.Key, event.OldValue)
		case hz.EntryAllCleared:
			fmt.Printf("MultiMap: %s was cleared.\n", event.MapName)
		}
	}
	// Observation on configuration setting with IncludeValue.
	ObserveListenerIncludeValueOnly(ctx, m, myHandler)
	// Observation on configuration setting on a specific Key with IncludeValue.
	ObserveListenerOnKey(ctx, m, myHandler)
}
