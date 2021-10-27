package main

import (
	"context"
	"fmt"
	"log"

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
	//error handling is omitted for brevity
	m, _ := client.GetMap(ctx, "myMap")
	// Populate map
	m.Put(ctx, "key1", 3)
	m.Put(ctx, "key2", 8)

	if _, err = m.ExecuteOnEntriesWithPredicate(ctx, &IdentifiedEntryProcessor{value: "test"},
		predicate.Between("this", 0, 5)); err != nil {
		log.Fatal(err)
	}

	value, _ := m.Get(ctx, "key1")
	fmt.Println("key1:", value)
	value, _ = m.Get(ctx, "key2")
	fmt.Println("key2:", value)
	// Shutdown client
	client.Shutdown(ctx)
}
