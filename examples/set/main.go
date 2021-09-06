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
	setItems := 10

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

	// Add messages to set
	for i := 0; i < setItems; i++ {
		set.Add(ctx, fmt.Sprintf("Item %d", i))
	}

	// Print contents of the set
	items, err := set.GetAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, val := range items {
		fmt.Println(val)
	}
}
