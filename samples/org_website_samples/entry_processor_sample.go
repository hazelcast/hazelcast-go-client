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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	IncEntryProcessorClassId   = 1
	IncEntryProcessorFactoryId = 66
)

type IncEntryProcessor struct {
}

func (p *IncEntryProcessor) ReadData(input *serialization.DataInput) error {
	return nil
}

func (p *IncEntryProcessor) WriteData(output *serialization.DataOutput) error {
	return nil
}

func (p *IncEntryProcessor) FactoryId() int32 {
	return IncEntryProcessorFactoryId
}

func (p *IncEntryProcessor) ClassId() int32 {
	return IncEntryProcessorClassId
}

func entryProcessorSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	clientConfig := hazelcast.NewHazelcastConfig()
	entryProcessor := &IncEntryProcessor{}
	hz, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)
	// Get the Distributed Map from Cluster.
	mp, _ := hz.GetMap("my-distributed-map")
	// Put the integer value of 0 into the Distributed Map
	mp.Put("key", 0)
	// Run the IncEntryProcessor class on the Hazelcast Cluster Member holding the key called "key"
	mp.ExecuteOnKey("key", entryProcessor)
	// Show that the IncEntryProcessor updated the value.
	newValue, _ := mp.Get("key")
	fmt.Println("new value:", newValue)
	// Shutdown this hazelcast client
	hz.Shutdown()
}
