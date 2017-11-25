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
)

const (
	sampleClassId   = 1
	sampleFactoryId = 1
)

// engineer implements IdentifiedDataSerializable interface.
// This means its serialization type will be IdentifiedDataSerializable.
type engineer struct {
	name      string
	surname   string
	age       int32
	languages []string
}

func (e *engineer) ReadPortable(reader serialization.PortableReader) error {
	var err error
	e.name, err = reader.ReadUTF("name")
	if err != nil {
		return err
	}
	e.surname, err = reader.ReadUTF("surname")
	if err != nil {
		return err
	}
	e.age, err = reader.ReadInt32("age")
	if err != nil {
		return err
	}
	e.languages, err = reader.ReadUTFArray("languages")
	if err != nil {
		return err
	}
	return nil
}
func (e *engineer) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("name", e.name)
	writer.WriteUTF("surname", e.surname)
	writer.WriteInt32("age", e.age)
	writer.WriteUTFArray("languages", e.languages)
	return nil
}

func (e *engineer) FactoryId() int32 {
	return sampleFactoryId
}

func (e *engineer) ClassId() int32 {
	return sampleClassId
}

type engineerFactory struct {
}

func (*engineerFactory) Create(classId int32) serialization.Portable {
	if classId == sampleClassId {
		return &engineer{}
	}
	return nil
}

func main() {
	var err error
	config := hazelcast.NewHazelcastConfig()

	en := &engineer{"Furkan", "Şenharputlu", 22, []string{"Turkish", "English", "Arabic"}}
	enFactory := &engineerFactory{}

	config.SerializationConfig().AddPortableFactory(en.FactoryId(), enFactory)
	client, err := hazelcast.NewHazelcastClientWithConfig(config)
	if err != nil {
		log.Println(err)
	}

	mp, err := client.GetMap("testMap")
	if err != nil {
		log.Println(err)
	}

	mp.Put("engineer1", en)
	ret, err := mp.Get("engineer1")
	retEngineer := ret.(*engineer)
	if err != nil {
		log.Println(err)
	}
	fmt.Println(retEngineer.name, retEngineer.surname, retEngineer.age, retEngineer.languages)

	mp.Clear()
	client.Shutdown()
}
