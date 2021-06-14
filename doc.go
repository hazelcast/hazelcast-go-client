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
