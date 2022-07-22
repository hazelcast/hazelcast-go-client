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
	// Start the client with defaults.
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Shutdown(ctx)
	// Get a random map.
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}
	// Populate map with an entry that will expire in 20 secs.
	if err := m.SetWithTTL(ctx, "key", "value", time.Second*20); err != nil {
		log.Fatal(err)
	}
	// Check whether entry is still in the map with 1 second intervals.
	for range time.Tick(time.Second) {
		exists, err := m.ContainsKey(ctx, "key")
		if err != nil {
			log.Fatal(err)
		}
		if !exists {
			break
		}
		log.Println("entry is still in the map...")
	}
	log.Println("entry expired!")
}
