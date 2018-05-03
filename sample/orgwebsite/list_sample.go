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

func listSampleRun() {

	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClient()
	// Get the distributed list from cluster
	list, _ := hz.GetList("my-distributed-list")
	// Add elements to the list
	list.Add("item1")
	list.Add("item2")
	// Remove the first element
	removed, _ := list.RemoveAt(0)
	fmt.Println("removed: ", removed)
	// There is only one element left
	size, _ := list.Size()
	fmt.Println("current size is: ", size)
	// Clear the list
	list.Clear()
	// Shutdown this hazelcast client
	hz.Shutdown()
}
