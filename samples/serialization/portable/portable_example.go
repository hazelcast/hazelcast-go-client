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
	return err
}

func (e *engineer) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("name", e.name)
	writer.WriteUTF("surname", e.surname)
	writer.WriteInt32("age", e.age)
	writer.WriteUTFArray("languages", e.languages)
	return nil
}

func (e *engineer) FactoryID() int32 {
	return sampleFactoryID
}

func (e *engineer) ClassID() int32 {
	return sampleClassID
}

type engineerFactory struct {
}

func (*engineerFactory) Create(classID int32) serialization.Portable {
	if classID == sampleClassID {
		return &engineer{}
	}
	return nil
}

func main() {
	var err error
	config := hazelcast.NewHazelcastConfig()

	en := &engineer{"Furkan", "Şenharputlu", 22, []string{"Turkish", "English", "Arabic"}}
	enFactory := &engineerFactory{}

	config.SerializationConfig().AddPortableFactory(en.FactoryID(), enFactory)
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
	if err != nil {
		log.Println(err)
	}
	retEngineer := ret.(*engineer)
	fmt.Println(retEngineer.name, retEngineer.surname, retEngineer.age, retEngineer.languages)

	mp.Clear()
	client.Shutdown()
}
