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
	. "github.com/hazelcast/hazelcast-go-client"
	. "github.com/hazelcast/hazelcast-go-client/config"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	employeeClassId                 = 100
	sampleDataSerializableFactoryId = 1000
)

type Employee struct {
	id   int32
	name string
}

func (e *Employee) ClassId() int32 {
	return employeeClassId
}

func (e *Employee) FactoryId() int32 {
	return sampleDataSerializableFactoryId
}

func (e *Employee) ReadData(input DataInput) (err error) {
	e.id, err = input.ReadInt32()
	if err != nil {
		return
	}
	e.name, err = input.ReadUTF()
	return
}
func (e *Employee) WriteData(output DataOutput) (err error) {
	output.WriteInt32(e.id)
	output.WriteUTF(e.name)
	return
}

type SampleDataSerializableFactory struct {
}

func (*SampleDataSerializableFactory) Create(classId int32) IdentifiedDataSerializable {
	if classId == classId {
		return &Employee{}
	}
	return nil
}

func identifiedDataSerializableSampleRun() {
	clientConfig := NewClientConfig()
	clientConfig.SerializationConfig().AddDataSerializableFactory(sampleDataSerializableFactoryId, &SampleDataSerializableFactory{})
	NewHazelcastClientWithConfig(clientConfig)
}
