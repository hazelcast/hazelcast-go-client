package main

import "fmt"
import "github.com/hazelcast/go-client"

/* This is the bare minimum test to see if we can connect.
 *
 * Steps here are:
 *
 * (1) Create 'clientConfig', configuration for the connection
 * (2) Add the server's host & port to the connection configuration
 * (3) Create a client using this configuration
 * (4) Disconnect the client and shut down.
 */

func main() {
	clientConfig := hazelcast.NewHazelcastConfig()
	fmt.Println("Cluster name: ", clientConfig.GroupConfig().Name())

	clientConfig.ClientNetworkConfig().AddAddress("127.0.0.1:5701")
	fmt.Println("Cluster discovery: ", clientConfig.ClientNetworkConfig().Addresses())

	client, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)

	client.Shutdown()
}
