//go:build ignore
// +build ignore

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
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

// messageListener handles incoming messages to the topic
func messageListener(event *hazelcast.MessagePublished) {
	fmt.Println("Received message: ", event.Value)
}

func main() {
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
	_, err = topic.AddMessageListener(ctx, messageListener)
	if err != nil {
		log.Fatal(err)
	}
	// Publish messages to topic
	for i := 0; i < 10; i++ {
		err = topic.Publish(ctx, fmt.Sprintf("Message %d", i))
		if err != nil {
			log.Fatal(err)
		}
	}
	// Shutdown client
	client.Shutdown(ctx)
}
