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

	// Get a random replicated map
	rand.Seed(time.Now().Unix())
	replicatedMapName := fmt.Sprintf("sample-%d", rand.Int())
	replicatedMap, err := client.GetReplicatedMap(ctx, replicatedMapName)
	if err != nil {
		log.Fatal(err)
	}

	replacedValue, err := replicatedMap.Put(ctx, "key", "value")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(replacedValue)

	value, err := replicatedMap.Get(ctx, "key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value)
}
