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
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"time"
)

const (
	customerClassId         = 1
	samplePortableFactoryId = 1
)

type Customer struct {
	name      string
	id        int32
	lastOrder time.Time
}

func (customer *Customer) FactoryId() int32 {
	return samplePortableFactoryId
}

func (customer *Customer) ClassId() int32 {
	return customerClassId
}

func (customer *Customer) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteInt32("id", customer.id)
	writer.WriteUTF("name", customer.name)
	writer.WriteInt64("lastOrder", customer.lastOrder.UnixNano()/int64(time.Millisecond))
	return
}

func (customer *Customer) ReadPortable(reader serialization.PortableReader) (err error) {
	customer.id, err = reader.ReadInt32("id")
	if err != nil {
		return
	}
	customer.name, err = reader.ReadUTF("name")
	if err != nil {
		return
	}
	t, err := reader.ReadInt64("lastOrder")
	if err != nil {
		return
	}
	customer.lastOrder = time.Unix(0, t*int64(time.Millisecond))
	return
}

type SamplePortableFactory struct {
}

func (pf *SamplePortableFactory) Create(classId int32) serialization.Portable {
	if classId == samplePortableFactoryId {
		return &Customer{}
	}
	return nil
}

func portableSerializableSampleRun() {
	clientConfig := config.NewClientConfig()
	clientConfig.SerializationConfig().AddPortableFactory(samplePortableFactoryId, &SamplePortableFactory{})
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewHazelcastClientWithConfig(clientConfig)
	// Customer can be used here

	// Shutdown this hazelcast client
	hz.Shutdown()
}
