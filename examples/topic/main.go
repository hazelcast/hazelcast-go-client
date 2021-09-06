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
	messageCount := 10

	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Get a random map.
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
}
