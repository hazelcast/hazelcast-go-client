package hztypes_test

import (
	"log"

	"github.com/hazelcast/hazelcast-go-client"
)

func Example() {
	// Start the client.
	client, err := hazelcast.StartNewClient()
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve a map.
	peopleMap, err := client.GetMap("people")
	if err != nil {
		log.Fatal(err)
	}
	// Call map functions.
	err = peopleMap.Set("jane", "doe")
	if err != nil {
		log.Fatal(err)
	}
}
