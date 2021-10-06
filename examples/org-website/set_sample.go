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
	// Get a random set
	rand.Seed(time.Now().Unix())
	setName := fmt.Sprintf("sample-%d", rand.Int())
	set, err := client.GetSet(ctx, setName)
	if err != nil {
		log.Fatal(err)
	}
	// Add items to set
	for i := 0; i < 10; i++ {
		// First returned value of Add() is a boolean describing
		// if the value was already in the set.
		_, err := set.Add(ctx, fmt.Sprintf("Item %d", i))
		if err != nil {
			log.Fatal(err)
		}
	}
	// Print contents of the set
	items, err := set.GetAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, val := range items {
		fmt.Println(val)
	}
	// Shutdown client
	client.Shutdown(ctx)
}
