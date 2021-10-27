package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
)

func main() {
	defaultConf := hazelcast.NewConfig()
	defaultConf.Serialization.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, defaultConf)
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
	// Populate map
	_, err = m.Put(ctx, "key1", 3)
	if err != nil {
		log.Fatal(err)
	}
	_, err = m.Put(ctx, "key2", 8)
	if err != nil {
		log.Fatal(err)
	}
	value, err := m.Get(ctx, "key1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("key1:", value)
	value, err = m.Get(ctx, "key2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("key2:", value)

	if _, err = m.ExecuteOnEntriesWithPredicate(ctx, &IdentifiedEntryProcessor{value: "test"},
		predicate.Between("this", 0, 5)); err != nil {
		log.Fatal(err)
	}

	value, err = m.Get(ctx, "key1")
	if err != nil {
		fmt.Println("key1:", value)
	}
	fmt.Println(value)
	value, err = m.Get(ctx, "key2")
	if err != nil {
		fmt.Println("key2:", value)
	}
	fmt.Println(value)
	// Shutdown client
	client.Shutdown(ctx)
}
