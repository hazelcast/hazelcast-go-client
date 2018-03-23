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
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
)

type CustomSerializableType struct {
	value string
}

type CustomSerializer struct {
}

func (s *CustomSerializer) Id() int32 {
	return 10
}

func (s *CustomSerializer) Read(input DataInput) (obj interface{}, err error) {
	array, err := input.ReadByteArray()
	return &CustomSerializableType{string(array)}, err
}

func (s *CustomSerializer) Write(output DataOutput, obj interface{}) (err error) {
	array := []byte(obj.(CustomSerializableType).value)
	output.WriteByteArray(array)
	return
}

func customSerializerSampleRun() {
	clientConfig := NewHazelcastConfig()
	clientConfig.SerializationConfig().AddCustomSerializer(reflect.TypeOf((*CustomSerializableType)(nil)), &CustomSerializer{})

	hz, _ := NewHazelcastClientWithConfig(clientConfig)
	mp, _ := hz.GetMap("customMap")
	mp.Put(1, &CustomSerializableType{"fooooo"})

	// Shutdown the Hazelcast Cluster Member
	hz.Shutdown()
}
