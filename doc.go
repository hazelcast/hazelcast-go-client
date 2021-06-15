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

/*
Package hazelcast provides the Hazelcast Go client.

Hazelcast Cloud Discovery

Hazelcast Go client can discover and connect to Hazelcast clusters running on Hazelcast Cloud https://cloud.hazelcast.com.
In order to activate it, set the cluster name, enable Hazelcast Cloud discovery and add Hazelcast Cloud Token to the configuration.
Here is an example:

	config := hazelcast.NewConfig()
	config.ClusterConfig.Name = "MY-CLUSTER-NAME"
	cc := &config.ClusterConfig.HazelcastCloudConfig
	cc.Enabled = true
	cc.Token = "MY-CLUSTER-TOKEN"
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}

Also check the code sample in https://github.com/hazelcast/hazelcast-go-client/tree/master/examples/discovery/cloud.

If you have enabled encryption for your cluster, you should also enable TLS/SSL configuration for the client.

External Client Public Address Discovery

When you set up a Hazelcast cluster in the Cloud (AWS, Azure, GCP, Kubernetes) and would like to use it from outside the Cloud network,
the client needs to communicate with all cluster members via their public IP addresses.
Whenever Hazelcast cluster members are able to resolve their own public external IP addresses, they pass this information to the client.
As a result, the client can use public addresses for communication, if it cannot access members via private IPs.

Hazelcast Go client has a built-in mechanism to use public IP addresses instead of private ones.
You can enable this feature by setting config.DiscoveryConfig.UsePublicIP to true and specifying the adddress of at least one member:

	config := hazelcast.NewConfig()
	cc := &config.ClusterConfig
	cc.SetAddress("30.40.50.60:5701")
	cc.DiscoveryConfig.UsePublicIP = true

For more details on member-side configuration, refer to the Discovery SPI section in the Hazelcast IMDG Reference Manual.

Listening for Distributed Object Events

You can listen to creation and destroy events for distributed objects by attaching a listener to the client.
A distributed object is created when first referenced unless it already exists.
Here is an example:

	// Error handling is omitted for brevity.
	handler := func(e hazelcast.DistributedObjectNotified) {
		isMapEvent := e.ServiceName == hazelcast.ServiceNameMap
		isCreationEvent := e.EventType == hazelcast.DistributedObjectCreated
		log.Println(e.EventType, e.ServiceName, e.ObjectName, "creation?", isCreationEvent, "isMap?", isMapEvent)
	}
	subscriptionID, _ := client.AddDistributedObjectListener(handler)
	myMap, _ := client.GetMap("my-map")
	// handler is called with: ServiceName=ServiceNameMap; ObjectName="my-map"; EventType=DistributedObjectCreated
	myMap.Destroy()
	// handler is called with: ServiceName=ServiceNameMap; ObjectName="my-map"; EventType=DistributedObjectDestroyed

If you don't want to receive any distributed object events, use client.RemoveDistributedObjectListener:

	client.RemoveDistributedObjectListener(subscriptionID)

Collecting Statistics

Hazelcast Management Center can monitor your clients if client-side statistics are enabled.

You can enable statistics by setting config.StatsConfig.Enabled to true.
Optionally, the period of statistics collection can be set using config.StatsConfig.Period setting.

	config := hazelcast.NewConfig()
	config.StatsConfig.Enabled = true
	config.StatsConfig.Period = 1 * time.Second
	client, err := hazelcast.StartNewClientWithConfig(config)
*/
package hazelcast
