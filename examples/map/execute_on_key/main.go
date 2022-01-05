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
	// Populate map.
	if _, err = m.Put(ctx, "key1", "value"); err != nil {
		log.Fatal(err)
	}
	if _, err = m.Put(ctx, "key2", "value"); err != nil {
		log.Fatal(err)
	}
	// Only change value corresponding to "key1".
	prevVal, err := m.ExecuteOnKey(ctx, &IdentifiedEntryProcessor{value: "testOnKey"}, "key1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("previous value:", prevVal)
	// Observe value corresponding to "key1" changed, "key2" remains unchanged.
	checkValue(ctx, m, "key1")
	checkValue(ctx, m, "key2")
	// Change both entries.
	updatedTo, err := m.ExecuteOnKeys(ctx, &IdentifiedEntryProcessor{value: "testOnKeys"}, "key1", "key2")
	if err != nil {
		log.Fatal(err)
	}
	// ExecuteOnKeys returns updated results.
	fmt.Println(updatedTo)
	// check the values
	checkValue(ctx, m, "key1")
	checkValue(ctx, m, "key2")
	// Shutdown client
	client.Shutdown(ctx)
}

// Retrieve and print value corresponding to "key" from map "m"
func checkValue(ctx context.Context, m *hazelcast.Map, key string) {
	value, err := m.Get(ctx, key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s\n", key, value)
}
