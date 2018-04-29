package main

import "fmt"
import "github.com/hazelcast/hazelcast-go-client"

/* The routine writes data in Hazelcast and closes down.
 * If Hazelcast doesn't close down too, then the data stays
 * for "three.go" to use.
 *
 * We save "Hello World" in various languages
 */
func main() {
	// Connect
	clientConfig := hazelcast.NewHazelcastConfig()
	clientConfig.ClientNetworkConfig().AddAddress("127.0.0.1:5701")
	client, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)

	// The map is stored on the server but we can access it from the client
	mapName := "greetings"
	greetings, _ := client.Map(mapName)

	// 0 if first run, non-zero if Hazelcast has data already
	size, _ := greetings.Size()
	fmt.Printf("Map '%v' Size before %v\n", greetings.Name(), size)

	// Write or overwrite data in the map as if it was stored in the client
	greetings.Put("English", "hello world")
	greetings.Put("Spanish", "hola mundo")
	greetings.Put("Italian", "ciao mondo")
	greetings.Put("German", "hallo welt")
	greetings.Put("French", "bonjour monde")

	// 5 added, so at least 5 on the server side
	size, _ = greetings.Size()
	fmt.Printf("Map '%v' Size after %v\n", greetings.Name(), size)

	// Disconnect
	client.Shutdown()
}
