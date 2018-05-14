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

package orgwebsite

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

func replicatedMapSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewHazelcastClient()
	// Get a Replicated Map called "my-replicated-map"
	mp, _ := hz.GetReplicatedMap("my-replicated-map")
	// Put and Get a value from the Replicated Map
	replacedValue, _ := mp.Put("key", "value")     // key/value replicated to all members
	fmt.Println("replacedValue = ", replacedValue) // Will be null as its first update
	value, _ := mp.Get("key")                      // the value is retrieved from a random member in the cluster
	fmt.Println("value for key = ", value)
	// Shutdown this hazelcast client
	hz.Shutdown()
}
