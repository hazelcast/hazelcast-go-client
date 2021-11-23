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
	m, err := client.GetMultiMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate map
	if success, err := m.Put(ctx, "key", "value1"); err != nil {
		log.Fatal(err)
	} else if !success {
		log.Fatal("multi-map put operation failed")
	}
	if success, err := m.Put(ctx, "key", "value2"); err != nil {
		log.Fatal(err)
	} else if !success {
		log.Fatal("multi-map put operation failed")
	}

	// Get both values under the same key
	values, err := m.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}

	// We see values in an interface slice
	// []interface {}{"value2", "value1"}
	fmt.Printf("%#v", values)
}
