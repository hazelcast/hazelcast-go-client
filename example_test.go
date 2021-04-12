package hazelcast_test

import (
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/lifecycle"
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
	// Stop the client once you are done with it.
	client.Shutdown()
}

func ExampleNewClientWithConfig() {
	// Create a configuration builder.
	configBuilder := hazelcast.NewConfigBuilder()
	configBuilder.Cluster().
		SetName("my-cluster").
		SetMembers("192.168.1.42:5000", "192.168.1.42:5001")
	// Start the client with the configuration provider.
	client, err := hazelcast.StartNewClientWithConfig(configBuilder)
	if err != nil {
		log.Fatal(err)
	}
	// ...
	// Stop the client once you are done with it.
	client.Shutdown()
}

func ExampleClient_ListenLifecycleStateChange() {
	// Create a client without starting it.
	client, err := hazelcast.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	// Attach an event listener.
	client.ListenLifecycleStateChange(1, func(event lifecycle.StateChanged) {
		switch event.State {
		case lifecycle.StateStarting:
			log.Println("Received starting state.")
		case lifecycle.StateStarted:
			log.Println("Received started state.")
		case lifecycle.StateShuttingDown:
			log.Println("Received shutting down state.")
		case lifecycle.StateShutDown:
			log.Println("Received shut down state.")
		case lifecycle.StateClientConnected:
			log.Println("Received client connected state.")
		case lifecycle.StateClientDisconnected:
			log.Println("Received client disconnected state.")
		default:
			log.Println("Received unknown state:", event.State)
		}
	})
	// Start the client.
	if err := client.Start(); err != nil {
		log.Fatal(err)
	}
}
