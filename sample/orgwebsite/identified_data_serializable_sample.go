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
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	employeeClassID                 = 100
	sampleDataSerializableFactoryID = 1000
)

type Employee struct {
	id   int32
	name string
}

func (e *Employee) ClassID() int32 {
	return employeeClassID
}

func (e *Employee) FactoryID() int32 {
	return sampleDataSerializableFactoryID
}

func (e *Employee) ReadData(input serialization.DataInput) (err error) {
	e.id, err = input.ReadInt32()
	if err != nil {
		return
	}
	e.name, err = input.ReadUTF()
	return
}

func (e *Employee) WriteData(output serialization.DataOutput) (err error) {
	output.WriteInt32(e.id)
	output.WriteUTF(e.name)
	return
}

type SampleDataSerializableFactory struct {
}

func (*SampleDataSerializableFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == classID {
		return &Employee{}
	}
	return nil
}

func identifiedDataSerializableSampleRun() {
	clientConfig := config.New()
	clientConfig.SerializationConfig().AddDataSerializableFactory(sampleDataSerializableFactoryID, &SampleDataSerializableFactory{})
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClientWithConfig(clientConfig)

	// Employee can be used here

	// Shutdown this hazelcast client
	hz.Shutdown()
}
