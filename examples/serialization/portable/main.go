/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

/*
This sample demonstrates custom portable serialization.
*/

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const factoryID = 1
const employeeClassID = 1

type EmployeePortable struct {
	Name string
	Age  int32
}

func (e EmployeePortable) FactoryID() int32 {
	return factoryID
}

func (e EmployeePortable) ClassID() int32 {
	return employeeClassID
}

func (e EmployeePortable) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("name", e.Name)
	writer.WriteInt32("age", e.Age)
}

func (e *EmployeePortable) ReadPortable(reader serialization.PortableReader) {
	e.Name = reader.ReadString("name")
	e.Age = reader.ReadInt32("age")
}

type MyPortableFactory struct {
}

func (m MyPortableFactory) Create(classID int32) serialization.Portable {
	if classID == employeeClassID {
		return &EmployeePortable{}
	}
	return nil
}

func (m MyPortableFactory) FactoryID() int32 {
	return factoryID
}

type BaseObject struct {
	Employee *EmployeePortable
}

func (b BaseObject) FactoryID() int32 {
	return factoryID
}

func (b BaseObject) ClassID() int32 {
	return 2
}

func (b BaseObject) WritePortable(writer serialization.PortableWriter) {
	if b.Employee == nil {
		writer.WriteNilPortable("employee", factoryID, employeeClassID)
		return
	}
	writer.WritePortable("employee", b.Employee)
}

func (b *BaseObject) ReadPortable(reader serialization.PortableReader) {
	b.Employee = reader.ReadPortable("employee").(*EmployeePortable)
}

func main() {
	// create the configuration
	config := hazelcast.NewConfig()
	config.Serialization.PortableVersion = 1
	config.Serialization.AddPortableFactory(&MyPortableFactory{})
	classDefinition := serialization.NewClassDefinition(1, 1, 1)
	classDefinition.AddStringField("name")
	classDefinition.AddInt32Field("age")
	config.Serialization.AddClassDefinition(classDefinition)

	// start the client with the given configuration
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	// retrieve a map
	m, err := client.GetMap(context.Background(), "example")
	if err != nil {
		log.Fatal(err)
	}
	// set / get portable serialized value
	employee := &EmployeePortable{Name: "Jane Doe", Age: 30}
	if err := m.Set(context.Background(), "employee", employee); err != nil {
		log.Fatal(err)
	}
	if v, err := m.Get(context.Background(), "employee"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(v)
	}
	// set / get portable serialized nullable value
	baseObj := &BaseObject{Employee: employee}
	if err := m.Set(context.Background(), "base-obj", baseObj); err != nil {
		log.Fatal(err)
	}
	if v, err := m.Get(context.Background(), "base-obj"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(v)
	}
	// stop the client
	time.Sleep(1 * time.Second)
	client.Shutdown()
}
