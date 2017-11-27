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
	"github.com/hazelcast/go-client"
	"github.com/hazelcast/go-client/serialization"
	"log"
	"reflect"
)

const (
	customSerializerId = 4 // customSerializerId should be greater than 0 and be specific to just one serializer.
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

func (s *CustomSerializer) Id() int32 {
	return customSerializerId
}

func (s *CustomSerializer) Read(input serialization.DataInput) (interface{}, error) {
	obj := &timeOfDay{}
	unit, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	obj.second = unit % 60
	unit = (unit - obj.second) / 60
	obj.minute = unit % 60
	unit = (unit - obj.minute) / 60
	obj.hour = unit
	return obj, nil
}

func (s *CustomSerializer) Write(output serialization.DataOutput, obj interface{}) error {
	secondPoint := (obj.(*timeOfDay).hour*60+obj.(*timeOfDay).minute)*60 + obj.(*timeOfDay).second
	output.WriteInt32(secondPoint)
	return nil
}

func main() {
	var err error
	config := hazelcast.NewHazelcastConfig()

	time := &timeOfDay{10, 12, 47}

	config.SerializationConfig().AddCustomSerializer(reflect.TypeOf(time), &CustomSerializer{})
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
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
