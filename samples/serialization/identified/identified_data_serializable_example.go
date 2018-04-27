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
	"fmt"
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	sampleClassID   = 1
	sampleFactoryID = 1
)

// student implements IdentifiedDataSerializable interface.
// This means its serialization type will be IdentifiedDataSerializable.
type student struct {
	id      int16
	name    string
	surname string
	gpa     float32
}

func (s *student) ReadData(input serialization.DataInput) error {
	var err error
	s.id, err = input.ReadInt16()
	if err != nil {
		return err
	}
	s.name, err = input.ReadUTF()
	if err != nil {
		return err
	}
	s.surname, err = input.ReadUTF()
	if err != nil {
		return err
	}
	s.gpa, err = input.ReadFloat32()
	return err
}

func (s *student) WriteData(output serialization.DataOutput) error {
	output.WriteInt16(s.id)
	output.WriteUTF(s.name)
	output.WriteUTF(s.surname)
	output.WriteFloat32(s.gpa)
	return nil
}

func (s *student) FactoryID() int32 {
	return sampleFactoryID
}

func (s *student) ClassID() int32 {
	return sampleClassID
}

type studentFactory struct {
}

func (*studentFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == sampleClassID {
		return &student{}
	}
	return nil
}

func main() {
	var err error
	config := hazelcast.NewHazelcastConfig()

	st := &student{10, "Furkan", "Şenharputlu", 3.5}
	stFactory := &studentFactory{}

	config.SerializationConfig().AddDataSerializableFactory(st.FactoryID(), stFactory)
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")
	if err != nil {
		log.Println(err)
	}

	mp.Put("student1", st)
	ret, err := mp.Get("student1")
	retStudent := ret.(*student)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(retStudent.id, retStudent.name, retStudent.surname, retStudent.gpa)

	mp.Clear()
	client.Shutdown()
}
