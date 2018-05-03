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

func multimapSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClient()
	// Get the Distributed MultiMap from Cluster.
	multiMap, _ := hz.GetMultiMap("myDistributedMultimap")
	// Put values in the map against the same key
	multiMap.Put("my-key", "value1")
	multiMap.Put("my-key", "value2")
	multiMap.Put("my-key", "value3")
	// Print out all the values for associated with key called "my-key"
	values, _ := multiMap.Get("my-key")
	fmt.Println(values)
	// remove specific key/value pair
	multiMap.Remove("my-key", "value2")
	// Shutdown this hazelcast client
	hz.Shutdown()
}
