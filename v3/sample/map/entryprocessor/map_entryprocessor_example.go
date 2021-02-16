// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/serialization"
)

const value = "value"

// EntryProcessor should be implemented on the server side.
// EntryProcessor should be registered to serialization.
func main() {
	config := hazelcast.NewConfig()
	processor := newSimpleEntryProcessor()
	config.SerializationConfig().AddDataSerializableFactory(processor.identifiedFactory.factoryID, processor.identifiedFactory)
	client, _ := hazelcast.NewClientWithConfig(config)

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
	classID           int32
	value             string
	identifiedFactory *identifiedFactory
}

func newSimpleEntryProcessor() *simpleEntryProcessor {
	processor := &simpleEntryProcessor{classID: 1, value: value}
	identifiedFactory := &identifiedFactory{factoryID: 66, simpleEntryProcessor: processor}
	processor.identifiedFactory = identifiedFactory
	return processor
}

type identifiedFactory struct {
	simpleEntryProcessor *simpleEntryProcessor
	factoryID            int32
}

func (idf *identifiedFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == idf.simpleEntryProcessor.classID {
		return &simpleEntryProcessor{classID: 1}
	}
	return nil
}

func (p *simpleEntryProcessor) ReadData(input serialization.DataInput) error {
	p.value = input.ReadUTF()
	return input.Error()
}

func (p *simpleEntryProcessor) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(p.value)
	return nil
}

func (p *simpleEntryProcessor) FactoryID() int32 {
	return p.identifiedFactory.factoryID
}

func (p *simpleEntryProcessor) ClassID() int32 {
	return p.classID
}
