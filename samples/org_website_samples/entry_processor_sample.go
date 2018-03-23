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
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"log"
)

type IncEntryProcessor struct {
	classId                               int32
	entryProcessorDataSerializableFactory *EntryProcessorDataSerializableFactory
}

func newIncEntryProcessor() *IncEntryProcessor {
	processor := &IncEntryProcessor{classId: 1}
	identifiedFactory := &EntryProcessorDataSerializableFactory{factoryId: 66, simpleEntryProcessor: processor}
	processor.entryProcessorDataSerializableFactory = identifiedFactory
	return processor
}

type EntryProcessorDataSerializableFactory struct {
	simpleEntryProcessor *IncEntryProcessor
	factoryId            int32
}

func (identifiedFactory *EntryProcessorDataSerializableFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == identifiedFactory.simpleEntryProcessor.classId {
		return &IncEntryProcessor{classId: 1}
	} else {
		return nil
	}
}

func (simpleEntryProcessor *IncEntryProcessor) ReadData(input serialization.DataInput) error {
	return nil
}

func (simpleEntryProcessor *IncEntryProcessor) WriteData(output serialization.DataOutput) error {
	return nil
}

func (simpleEntryProcessor *IncEntryProcessor) FactoryId() int32 {
	return simpleEntryProcessor.entryProcessorDataSerializableFactory.factoryId
}

func (simpleEntryProcessor *IncEntryProcessor) ClassId() int32 {
	return simpleEntryProcessor.classId
}

func entryProcessorSampleRun() {
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	clientConfig := hazelcast.NewHazelcastConfig()
	entryProcessor := newIncEntryProcessor()
	clientConfig.SerializationConfig().
		AddDataSerializableFactory(entryProcessor.entryProcessorDataSerializableFactory.factoryId,
			entryProcessor.entryProcessorDataSerializableFactory)
	hz, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)
	// Get the Distributed Map from Cluster.
	mp, _ := hz.GetMap("my-distributed-map")
	// Put the integer value of 0 into the Distributed Map
	mp.Put("key", 0)
	// Run the IncEntryProcessor class on the Hazelcast Cluster Member holding the key called "key"
	mp.ExecuteOnKey("key", entryProcessor)
	// Show that the IncEntryProcessor updated the value.
	newValue, _ := mp.Get("key")
	log.Println("new value:", newValue)
	// Shutdown the Hazelcast Cluster Member
	hz.Shutdown()
}
