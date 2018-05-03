package main

import "fmt"
import "github.com/hazelcast/hazelcast-go-client"

/* The routine reads data from Hazelcast, that was placed there
 * by two.go
 */
func main() {
	// Connect
	clientConfig := hazelcast.NewConfig()
	clientConfig.NetworkConfig().AddAddress("127.0.0.1:5701")
	client, _ := hazelcast.NewClientWithConfig(clientConfig)

	// The map is stored on the server but we can access it from the client
	mapName := "greetings"
	greetings, _ := client.GetMap(mapName)

	// A map is a key-value store, not sorted by default
	keys, _ := greetings.KeySet()

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		value, _ := greetings.Get(key)
		fmt.Printf(" -> '%v'=='%v'\n", key, value)
	}

	// Count what was returned
	size := len(keys)
	s := ""
	if size != 1 {
		s = "s"
	}
	fmt.Printf("[%v record%v]\n", size, s)

	// Disconnect
	client.Shutdown()
}
