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

// messageListener handles incoming messages to the topic.
func messageListener(event *hazelcast.MessagePublished) {
	fmt.Println("Received message: ", event.Value)
}

func main() {
	ctx := context.TODO()
	// Start the client with defaults.
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(err)
	}
	// Get a reference to the queue.
	myTopic, err := client.GetTopic(ctx, "my-topic")
	if err != nil {
		panic(err)
	}
	// Add a message listener to the topic.
	_, err = myTopic.AddMessageListener(ctx, messageListener)
	if err != nil {
		panic(err)
	}
	// Publish messages to topic
	for i := 0; i < 10; i++ {
		err = myTopic.Publish(ctx, fmt.Sprintf("Message %d", i))
		if err != nil {
			panic(err)
		}
	}
	// Shutdown the client.
	client.Shutdown(ctx)
}
