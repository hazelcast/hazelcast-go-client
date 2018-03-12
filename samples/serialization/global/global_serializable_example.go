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

package main

import (
	"encoding/json"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"log"
)

const (
	globalSerializerId = 5 // globalSerializerId should be greater than 0 and be specific to just one serializer.
)

type colorGroup struct {
	ID     int
	Name   string
	Colors []string
}

// GlobalSerializer will handle all struct types if all the steps in searching for a serializer fail.
// If none of custom and global serializers are not added to SerializationConfig, objects will be serialized by default GoLang Gob Serializer.
// For example, here JSON package's serialization is used.
type GlobalSerializer struct {
}

func (s *GlobalSerializer) Id() int32 {
	return globalSerializerId
}

func (s *GlobalSerializer) Read(input serialization.DataInput) (interface{}, error) {
	var err error
	jsonBlob, err := input.ReadByteArray()
	if err != nil {
		return nil, err
	}
	var ret colorGroup
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *GlobalSerializer) Write(output serialization.DataOutput, obj interface{}) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	output.WriteByteArray(b)
	return nil
}

func main() {
	var err error
	config := hazelcast.NewHazelcastConfig()

	group := colorGroup{
		ID:     1,
		Name:   "Reds",
		Colors: []string{"Crimson", "Red", "Ruby", "Maroon"},
	}

	config.SerializationConfig().SetGlobalSerializer(&GlobalSerializer{})
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")
	if err != nil {
		log.Println(err)
	}

	mp.Put("group1", group)
	ret, err := mp.Get("group1")
	retGroup := ret.(colorGroup)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Color group is", retGroup)

	mp.Clear()
	client.Shutdown()
}
