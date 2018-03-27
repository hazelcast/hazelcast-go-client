// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org_website_samples

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

func queueSampleRun() {

	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewHazelcastClient()
	// Get a Blocking Queue called "my-distributed-queue"
	queue, _ := hz.GetQueue("my-distributed-queue")
	// Offer a String into the Distributed Queue
	queue.Offer("item")
	// Poll the Distributed Queue and return the String
	queue.Poll()
	//Timed blocking Operations
	queue.OfferWithTimeout("anotheritem", 500, time.Millisecond)
	queue.PollWithTimeout(5, time.Second)
	//Indefinitely blocking Operations
	queue.Put("yetanotheritem")
	fmt.Println(queue.Take())
	// Shutdown this hazelcast client
	hz.Shutdown()
}
