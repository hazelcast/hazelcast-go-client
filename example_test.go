/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast_test

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

/*
func ExampleClient_ListenLifecycleStateChange() {
	// Create a client without starting it.
	client, err := hazelcast.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	// Attach an event listener.
	client.ListenLifecycleStateChange(func(event hazelcast.LifecycleStateChanged) {
		switch event.State {
		case hazelcast.LifecycleStateStarting:
			log.Println("Received starting state.")
		case hazelcast.LifecycleStateStarted:
			log.Println("Received started state.")
		case hazelcast.LifecycleStateShuttingDown:
			log.Println("Received shutting down state.")
		case hazelcast.LifecycleStateShutDown:
			log.Println("Received shut down state.")
		case hazelcast.LifecycleStateClientConnected:
			log.Println("Received client connected state.")
		case hazelcast.LifecycleStateClientDisconnected:
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

*/
