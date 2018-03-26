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
	"github.com/hazelcast/hazelcast-go-client"
	"log"
)

func setSampleRun() {

	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewHazelcastClient()
	// Get the distributed set from cluster
	set, _ := hz.GetSet("my-distributed-set")
	// Add items to the set with duplicates
	set.Add("item1")
	set.Add("item1")
	set.Add("item2")
	set.Add("item2")
	set.Add("item3")
	set.Add("item3")
	// Get the items. Note that no duplicates
	items, _ := set.ToSlice()
	for _, item := range items {
		log.Println(item)
	}

	hz.Shutdown()
}
