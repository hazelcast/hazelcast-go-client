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

func ObserveListenerIncludeValueOnly(ctx context.Context, m *hz.MultiMap, myHandler func(*hz.EntryNotified)) error {
	// In this observation, we will observe EntryAdded, EntryRemoved actions included their value.

	// Let's create a config for our listeners
	listenerConfig := hz.MultiMapEntryListenerConfig{IncludeValue: true}
	// Add desired notification through config
	listenerConfig.NotifyEntryAdded(true)
	listenerConfig.NotifyEntryRemoved(true)
	// Add our continuous entry listener to the MultiMap that we requested in main
	subscriptionID, err := m.AddEntryListener(ctx, listenerConfig, myHandler)
	if err != nil {
		return err
	}
	// Define our entries to observe MultiMap
	myEntries := []types.Entry{
		{Key: "my-key", Value: "my-value1"},
		{Key: "my-key", Value: "my-value2"},
		{Key: "my-key", Value: "my-value3"},
	}
	// Add those entries to our MultiMap.
	for _, entry := range myEntries {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			return err
		}
	}
	// Then, remove same entries to observe EntryAdded and EntryRemoved actions
	for _, entry := range myEntries {
		_, err := m.Remove(ctx, entry.Key)
		if err != nil {
			return err
		}
	}

	// If you observe the output, you can clearly see that our handler works fine and notify us for each "my-key" put and remove operation.
	// Also, notice that notification order is not guaranteed to be complied with the order of operation.

	// Remove our entry listener from our MultiMap
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		return err
	}
	return nil
}

func ObserveListenerOnKey(ctx context.Context, m *hz.MultiMap, myHandler func(*hz.EntryNotified)) error {
	// In this observation, we will observe EntryAllCleared action and also EntryAdded, EntryRemoved actions for on a certain key.

	// The key that we will listen on later.
	myAwesomeKey := "my-awesome-key"
	// Let's create a config for our listeners with our key
	listenerConfig := hz.MultiMapEntryListenerConfig{IncludeValue: true, Key: myAwesomeKey}
	// Add desired notification through config
	listenerConfig.NotifyEntryAllCleared(true)
	listenerConfig.NotifyEntryAdded(true)
	// Add our continuous entry listener to the MultiMap that we requested in main
	subscriptionID, err := m.AddEntryListener(ctx, listenerConfig, myHandler)
	if err != nil {
		return err
	}
	// These are the entries to observe that we only interested in the key which we defined above.
	// Events related to dummy key should not be notified and handled.
	myEntries := []types.Entry{
		{Key: myAwesomeKey, Value: "my-awesome-value1"},
		{Key: myAwesomeKey, Value: "my-awesome-value2"},
		{Key: myAwesomeKey, Value: "my-awesome-value3"},
		{Key: "my-dummy-key", Value: "my-dummy-value"},
	}
	// Put my observation entries to the MultiMap
	for _, entry := range myEntries {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			return err
		}
	}
	// To observe `clear` event, lets clear our map as well.
	if err := m.Clear(ctx); err != nil {
		return err
	}

	// If you observe the output, you can clearly see that we only handled myAwesomeKey related events then listener ignores
	// "my-dummy-key" related event.
	// Also, notice that notification order is not guaranteed to be complied with the order of operation.

	// Remove our entry listener from our MultiMap
	if err := m.RemoveEntryListener(ctx, subscriptionID); err != nil {
		return err
	}
	return nil
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
	// Request an instance of MultiMap
	m, err := client.GetMultiMap(ctx, "my-multimap")
	if err != nil {
		log.Fatal(err)
	}
	// Define a handler with supported EntryEventType for the upcoming events.
	myHandler := func(event *hz.EntryNotified) {
		switch event.EventType {
		case hz.EntryAdded:
			fmt.Printf("Multimap: %s, (key: %v, value: %v) was added.\n", event.MapName, event.Key, event.Value)
		case hz.EntryRemoved:
			fmt.Printf("Multimap: %s, (key: %v, value: %v) was removed.\n", event.MapName, event.Key, event.OldValue)
		case hz.EntryAllCleared:
			fmt.Printf("Multimap: %s was cleared.\n", event.MapName)
		}
	}
	// Observation on configuration setting with IncludeValue
	err = ObserveListenerIncludeValueOnly(ctx, m, myHandler)
	if err != nil {
		log.Fatal(err)
	}
	// Observation on configuration setting with IncludeValue and Key.
	err = ObserveListenerOnKey(ctx, m, myHandler)
	if err != nil {
		log.Fatal(err)
	}
}
