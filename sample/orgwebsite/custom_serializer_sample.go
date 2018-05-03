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
	"reflect"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type CustomSerializable struct {
	value string
}

type CustomSerializer struct {
}

func (s *CustomSerializer) ID() int32 {
	return 10
}

func (s *CustomSerializer) Read(input serialization.DataInput) (obj interface{}, err error) {
	array, err := input.ReadByteArray()
	return &CustomSerializable{string(array)}, err
}

func (s *CustomSerializer) Write(output serialization.DataOutput, obj interface{}) (err error) {
	array := []byte(obj.(CustomSerializable).value)
	output.WriteByteArray(array)
	return
}

func customSerializerSampleRun() {
	clientConfig := hazelcast.NewConfig()
	clientConfig.SerializationConfig().AddCustomSerializer(reflect.TypeOf((*CustomSerializable)(nil)), &CustomSerializer{})

	// Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
	hz, _ := hazelcast.NewClientWithConfig(clientConfig)
	// CustomSerializer will serialize/deserialize CustomSerializable objects

	// Shutdown this hazelcast client
	hz.Shutdown()
}
