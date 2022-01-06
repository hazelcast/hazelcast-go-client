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
	"log"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func main() {
	ctx := context.TODO()

	client, err := hz.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	m, err := client.GetMultiMap(ctx, "my-multimap")
	if err != nil {
		log.Fatal(err)
	}

	myHandler := func(event *hz.EntryNotified) {
		switch event.EventType {
		case hz.EntryAdded:
			log.Printf("Multimap: %s, (key: %v, value: %v) was added.\n", event.MapName, event.Key, event.Value)
		case hz.EntryRemoved:
			log.Printf("Multimap: %s, (key: %v, value: %v) was removed.\n", event.MapName, event.Key, event.OldValue)
		case hz.EntryAllCleared:
			log.Printf("Multimap: %s was cleared.\n", event.MapName)
		}
	}

	myAwesomeKey := "my-awesome-key"

	listenerConfigs := [...]hz.MultiMapEntryListenerConfig{
		{IncludeValue: true},
		{IncludeValue: true, Key: myAwesomeKey},
	}

	listenerConfigs[0].NotifyEntryAdded(true)
	listenerConfigs[0].NotifyEntryRemoved(true)

	subscriptionID1, err := m.AddEntryListener(ctx, listenerConfigs[0], myHandler)
	if err != nil {
		log.Fatal(err)
	}

	myEntries1 := []types.Entry{
		{Key: "my-key", Value: "my-value1"},
		{Key: "my-key", Value: "my-value2"},
		{Key: "my-key", Value: "my-value3"},
	}

	for _, entry := range myEntries1 {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, entry := range myEntries1 {
		_, err := m.Remove(ctx, entry.Key)
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := m.RemoveEntryListener(ctx, subscriptionID1); err != nil {
		log.Fatal(err)
	}

	listenerConfigs[1].NotifyEntryAllCleared(true)
	listenerConfigs[1].NotifyEntryAdded(true)
	subscriptionID2, err := m.AddEntryListener(ctx, listenerConfigs[1], myHandler)
	if err != nil {
		log.Fatal(err)
	}

	err = m.Clear(ctx)
	if err != nil {
		log.Fatal(err)
	}

	myEntries2 := []types.Entry{
		{Key: myAwesomeKey, Value: "my-awesome-value1"},
		{Key: myAwesomeKey, Value: "my-awesome-value2"},
		{Key: myAwesomeKey, Value: "my-awesome-value3"},
		{Key: "my-dummy-key", Value: "my-dummy-value"},
	}

	for _, entry := range myEntries2 {
		_, err := m.Put(ctx, entry.Key, entry.Value)
		if err != nil {
			log.Fatal(err)
		}
	}

	if err := m.RemoveEntryListener(ctx, subscriptionID2); err != nil {
		log.Fatal(err)
	}
}
