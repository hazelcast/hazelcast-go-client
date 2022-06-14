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
	"errors"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
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
	// Get a random Ringbuffer
	rand.Seed(time.Now().Unix())
	queueName := fmt.Sprintf("sample-%d", rand.Int())
	rb, err := client.GetRingbuffer(ctx, queueName)
	if err != nil {
		log.Fatal(err)
	}

	// *** Example adding and retrieving single items
	// Add an item to the Ringbuffer
	sequence, err := rb.Add(ctx, "item 1", hazelcast.OverflowPolicyOverwrite)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Added item 1 with sequence number=%d\n", sequence)
	// Get an item by a known sequence number
	item, err := rb.ReadOne(ctx, sequence)
	if err != nil {
		// example of validating for stale items
		if errors.Is(err, hzerrors.ErrStaleSequence) {
			log.Printf("The item with the sequence number = %d is no longer in the Ringbuffer", sequence)
		} else {
			log.Fatal(err)
		}
	}
	fmt.Println(item)

	// *** Example adding and reading multiple items
	// Add an item to the Ringbuffer
	lastSequence, err := rb.AddAll(ctx, hazelcast.OverflowPolicyOverwrite, "One", "Two", "Three")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Added 3 items, and the last item's sequence number=%d\n", lastSequence)
	// Good practice to define explicit timeouts for larger readings
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// Get multiple items
	resultSet, err := rb.ReadMany(ctxWithTimeout, sequence+1, 3, 3, nil)
	if err != nil {
		if ctxWithTimeout.Err() != nil {
			log.Fatal("timed out ... you might want to retry in a few seconds?")
		}
		log.Fatal(err)
	}
	for i := 0; i < int(resultSet.ReadCount()); i++ {
		item, _ := resultSet.Get(i)
		fmt.Println(item)
	}

	// Shutdown client
	client.Shutdown(ctx)
}
