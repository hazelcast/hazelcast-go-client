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

import "github.com/hazelcast/hazelcast-go-client"

func mapSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClient()
	// Get the Distributed Map from Cluster.
	mp, _ := hz.GetMap("myDistributedMap")
	//Standard Put and Get.
	mp.Put("key", "value")
	mp.Get("key")
	//Concurrent Map methods, optimistic updating
	mp.PutIfAbsent("somekey", "somevalue")
	mp.ReplaceIfSame("key", "value", "newvalue")
	// Shutdown this hazelcast client
	hz.Shutdown()
}
