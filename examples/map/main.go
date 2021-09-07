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

	// Get a random map
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}

	replacedValue, err := m.Put(ctx, "key", "value")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(replacedValue)

	value, err := m.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)

	m.PutIfAbsent(ctx, "key-2", "value-2")
	replaced, err := m.ReplaceIfSame(ctx, "key-2", "value-2", "value3")
	if err != nil {
		log.Fatal(err)
	}
	if replaced {
		fmt.Println("Replaced value")
	}
}
