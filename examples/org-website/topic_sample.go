//go:build ignore
// +build ignore

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
	// Add a message listeners to the topic
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
