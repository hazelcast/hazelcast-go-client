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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	handleError(err)
	// Shutdown client
	defer client.Shutdown(ctx)
	// Get a random Ringbuffer
	rand.Seed(time.Now().Unix())
	rbName := fmt.Sprintf("sample-%d", rand.Int())
	rb, err := client.GetRingbuffer(ctx, rbName)
	handleError(err)
	defer rb.Destroy(ctx)
	printRingbufferStats(rb, ctx)
	// Example adding and retrieving single items
	// Add an item to the Ringbuffer
	sequence, err := rb.Add(ctx, "item 1", hazelcast.OverflowPolicyOverwrite)
	handleError(err)
	fmt.Printf("Added item 1 with sequence number=%d\n", sequence)
	printRingbufferStats(rb, ctx)
	// Get an item by a known sequence number
	item, err := rb.ReadOne(ctx, sequence)
	if err != nil {
		// example of validating for stale items
		if !errors.Is(err, hzerrors.ErrStaleSequence) {
			panic(err)
		}
		log.Printf("The item with the sequence number = %d is no longer in the Ringbuffer", sequence)
	}
	fmt.Println(item)
	printRingbufferStats(rb, ctx)
	// Example adding and reading multiple items
	// Add an item to the Ringbuffer
	lastSequence, err := rb.AddAll(ctx, hazelcast.OverflowPolicyOverwrite, "One", "Two", "Three")
	handleError(err)
	fmt.Printf("Added 3 items, and the last item's sequence number=%d\n", lastSequence)
	printRingbufferStats(rb, ctx)
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
}

func printRingbufferStats(rb *hazelcast.Ringbuffer, ctx context.Context) {
	var (
		err   error
		stats struct {
			Size              int64
			Capacity          int64
			RemainingCapacity int64
			HeadSequence      int64
			TailSequence      int64
		}
	)
	stats.Size, err = rb.Size(ctx)
	handleError(err)
	stats.Capacity, err = rb.Capacity(ctx)
	handleError(err)
	stats.RemainingCapacity, err = rb.RemainingCapacity(ctx)
	handleError(err)
	stats.HeadSequence, err = rb.HeadSequence(ctx)
	handleError(err)
	stats.TailSequence, err = rb.TailSequence(ctx)
	handleError(err)
	s, err := json.MarshalIndent(stats, "", "  ")
	handleError(err)
	fmt.Printf("Ringbuffer %s: %s\n", rb.Name(), s)
}

func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
