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

package orgwebsite

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	customerClassID         = 1
	samplePortableFactoryID = 1
)

type Customer struct {
	name      string
	id        int32
	lastOrder time.Time
}

func (c *Customer) FactoryID() int32 {
	return samplePortableFactoryID
}

func (c *Customer) ClassID() int32 {
	return customerClassID
}

func (c *Customer) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteInt32("id", c.id)
	writer.WriteUTF("name", c.name)
	writer.WriteInt64("lastOrder", c.lastOrder.UnixNano()/int64(time.Millisecond))
	return
}

func (c *Customer) ReadPortable(reader serialization.PortableReader) (err error) {
	c.id = reader.ReadInt32("id")
	c.name = reader.ReadUTF("name")
	t := reader.ReadInt64("lastOrder")
	c.lastOrder = time.Unix(0, t*int64(time.Millisecond))
	return reader.Error()
}

type SamplePortableFactory struct {
}

func (pf *SamplePortableFactory) Create(classID int32) serialization.Portable {
	if classID == customerClassID {
		return &Customer{}
	}
	return nil
}

func portableSerializableSampleRun() {
	clientConfig := config.New()
	clientConfig.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &SamplePortableFactory{})
	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClientWithConfig(clientConfig)
	// Customer can be used here

	// Shutdown this hazelcast client
	hz.Shutdown()
}
