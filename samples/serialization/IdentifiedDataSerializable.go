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

package serialization

import (
	"github.com/hazelcast/go-client/serialization"
	"github.com/hazelcast/go-client"
	"log"
)

const (
	SAMPLE_CLASS_ID   = 1
	SAMPLE_FACTORY_ID = 1
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
	if err != nil {
		return err
	}
	return nil
}
func (s *student) WriteData(output serialization.DataOutput) error {
	output.WriteInt16(s.id)
	output.WriteUTF(s.name)
	output.WriteUTF(s.surname)
	output.WriteFloat32(s.gpa)
	return nil
}

func (s *student) FactoryId() int32 {
	return SAMPLE_FACTORY_ID
}

func (s *student) ClassId() int32 {
	return SAMPLE_CLASS_ID
}

type studentFactory struct {
}

func (*studentFactory) Create(classId int32) serialization.IdentifiedDataSerializable {
	if classId == SAMPLE_CLASS_ID {
		return &student{}
	}
	return nil
}

func putAndGetUser() *student{
	var err error
	config := hazelcast.NewHazelcastConfig()

	student := &student{10, "Furkan", "Şenharputlu", 3.5}
	studentFactory := &studentFactory{}

	config.SerializationConfig().AddDataSerializableFactory(student.FactoryId(), studentFactory)
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")
	if err != nil {
		log.Println(err)
	}

	mp.Put("student1", student)
	retStudent, err := mp.Get("student1")
	if err != nil {
		log.Println(err)
	}

	mp.Clear()
	client.Shutdown()
	return retStudent.(*student)
}
