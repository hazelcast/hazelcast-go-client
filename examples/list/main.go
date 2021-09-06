package main

import (
	"context"
	"fmt"
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

	// Get a random topic
	rand.Seed(time.Now().Unix())
	listName := fmt.Sprintf("sample-%d", rand.Int())
	list, err := client.GetList(ctx, listName)
	if err != nil {
		log.Fatal(err)
	}

	// Get and print list size
	size, err := list.Size(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(size)

	// Add data
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
