package main

import "fmt"
import "github.com/hazelcast/hazelcast-go-client"

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
	clientConfig := hazelcast.NewConfig()
	fmt.Println("Cluster name: ", clientConfig.GroupConfig().Name())

	clientConfig.NetworkConfig().AddAddress("127.0.0.1:5701")
	fmt.Println("Cluster discovery: ", clientConfig.NetworkConfig().Addresses())

	client, _ := hazelcast.NewClientWithConfig(clientConfig)

	client.Shutdown()
}
