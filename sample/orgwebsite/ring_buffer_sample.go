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
	"github.com/hazelcast/hazelcast-go-client/core"
)

func ringBufferSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClient()

	rb, _ := hz.GetRingbuffer("rb")
	// we start from the oldest item.
	// if you want to start from the next item, call rb.tailSequence()+1
	// add two items into ring buffer
	rb.Add(100, core.OverflowPolicyOverwrite)
	rb.Add(200, core.OverflowPolicyOverwrite)

	// we start from the oldest item.
	// if you want to start from the next item, call rb.tailSequence()+1
	sequence, _ := rb.HeadSequence()
	fmt.Println(rb.ReadOne(sequence))
	sequence++
	fmt.Println(rb.ReadOne(sequence))
	// Shutdown this hazelcast client
	hz.Shutdown()
}
