package hazelcast_test

import (
	"log"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
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
	configBuilder := hazelcast.NewClientConfigBuilder()
	configBuilder.Cluster().
		SetName("my-cluster").
		SetMembers("192.168.1.42:5000", "192.168.1.42:5001")
	// Create the configuration
	config, err := configBuilder.Config()
	if err != nil {
		log.Fatal(err)
	}
	// Start the client with the configuration.
	client, err := hazelcast.StartNewClientWithConfig(config)
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
	client.ListenLifecycleStateChange(func(event lifecycle.StateChanged) {
		switch event.State {
		case lifecycle.StateStarting:
			log.Println("Received starting state.")
		case lifecycle.StateStarted:
			log.Println("Received started state.")
		case lifecycle.StateShuttingDown:
			log.Println("Received shutting down state.")
		case lifecycle.StateShutDown:
			log.Println("Received shut down state.")
		case lifecycle.StateMerging:
			log.Println("Received merging state.")
		case lifecycle.StateMerged:
			log.Println("Received merged state.")
		case lifecycle.StateMergeFailed:
			log.Println("Received merge failed state.")
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
