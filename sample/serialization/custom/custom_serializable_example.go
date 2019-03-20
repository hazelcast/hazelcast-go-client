// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
	"log"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	customSerializerID = 4 // customSerializerID should be greater than 0 and be specific to just one serializer.
)

type timeOfDay struct {
	hour   int32
	minute int32
	second int32
}

// CustomSerializer may be for an interface type or just a specific struct type.
// In this example, CustomSerializer is for the timeOfDay struct.
type CustomSerializer struct {
}

func (*CustomSerializer) ID() int32 {
	return customSerializerID
}

func (s *CustomSerializer) Read(input serialization.DataInput) (interface{}, error) {
	obj := &timeOfDay{}
	unit := input.ReadInt32()
	obj.second = unit % 60
	unit = (unit - obj.second) / 60
	obj.minute = unit % 60
	unit = (unit - obj.minute) / 60
	obj.hour = unit
	return obj, input.Error()
}

func (s *CustomSerializer) Write(output serialization.DataOutput, obj interface{}) error {
	secondPoint := (obj.(*timeOfDay).hour*60+obj.(*timeOfDay).minute)*60 + obj.(*timeOfDay).second
	output.WriteInt32(secondPoint)
	return nil
}

func main() {
	var err error
	config := hazelcast.NewConfig()

	time := &timeOfDay{10, 12, 47}

	config.SerializationConfig().AddCustomSerializer(reflect.TypeOf(time), &CustomSerializer{})
	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")
	if err != nil {
		log.Println(err)
	}

	mp.Put("time1", time)
	ret, err := mp.Get("time1")
	retTime := ret.(*timeOfDay)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Time is", retTime.hour, ":", retTime.minute, ":", retTime.second)

	mp.Clear()
	client.Shutdown()
}
