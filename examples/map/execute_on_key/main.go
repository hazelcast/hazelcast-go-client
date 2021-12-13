package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	defaultConf := hazelcast.NewConfig()
	defaultConf.Serialization.SetIdentifiedDataSerializableFactories(&IdentifiedFactory{})
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, defaultConf)
	if err != nil {
		log.Fatal(err)
	}
	m, err := client.GetMap(ctx, "myMap")
	if err != nil {
		log.Fatal(err)
	}
	// Populate map
	if _, err = m.Put(ctx, "key1", "value"); err != nil {
		log.Fatal(err)
	}
	if _, err = m.Put(ctx, "key2", "value"); err != nil {
		log.Fatal(err)
	}
	// only change value corresponding to key1
	finalVal, err := m.ExecuteOnKey(ctx, &IdentifiedEntryProcessor{value: "test"}, "key1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("previous value:", finalVal)
	// check the values
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
	// Shutdown client
	client.Shutdown(ctx)
}
