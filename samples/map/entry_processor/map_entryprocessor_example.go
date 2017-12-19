// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package main

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// EntryProcessor should be implemented on the server side.
// EntryProcessor should be registered to serialization.
func main() {
	config := hazelcast.NewHazelcastConfig()
	expectedValue := "newValue"
	processor := newSimpleEntryProcessor(expectedValue)
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryId, processor.identifiedFactory)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)

	mp, _ := client.GetMap("testMap")
	mp.Put("testKey", "testValue")
	value, err := mp.ExecuteOnKey("testKey", processor)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("after processing the new value is ", value)

	newValue, _ := mp.Get("testingKey1")
	fmt.Println("after processing the new value is ", newValue)

	mp.Clear()
	client.Shutdown()
}

type simpleEntryProcessor struct {
	classId           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor(value string) *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classId: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryId: 66, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryId            int32
}

func (identifiedFactory *identifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == identifiedFactory.simpleEntryProcessor.classId {
		return &simpleEntryProcessor{classId: 1}
	} else {
		return nil
	}
}

func (simpleEntryProcessor *simpleEntryProcessor) ReadData(input serialization.DataInput) error {
	var err error
	simpleEntryProcessor.value, err = input.ReadUTF()
	return err
}

func (simpleEntryProcessor *simpleEntryProcessor) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(simpleEntryProcessor.value)
	return nil
}

func (simpleEntryProcessor *simpleEntryProcessor) FactoryId() int32 {
	return simpleEntryProcessor.identifiedFactory.factoryId
}

func (simpleEntryProcessor *simpleEntryProcessor) ClassId() int32 {
	return simpleEntryProcessor.classId
}
