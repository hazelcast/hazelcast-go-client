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

package serialization_test

import (
	"context"
	"reflect"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"

	"github.com/hazelcast/hazelcast-go-client/internal/it"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestPortableSerialize(t *testing.T) {
	it.MapTesterWithConfig(t, func(config *hz.Config) {
		config.SerializationConfig.AddPortableFactory(&portableFactory{})
	}, func(t *testing.T, m *hz.Map) {
		target := newEmployee("Ford Prefect", 33, true)
		it.Must(m.Set(context.Background(), "ford", target))
		if value, err := m.Get(context.Background(), "ford"); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(target, value) {
			t.Fatalf("target %#v: %#v", target, value)
		}
	})
}

type employee struct {
	Name   string
	Age    int
	Active bool
}

func newEmployee(name string, age int, active bool) *employee {
	return &employee{
		Name:   name,
		Age:    age,
		Active: active,
	}
}

func (e *employee) ReadPortable(reader serialization.PortableReader) {
	e.Name = reader.ReadString("Name")
	e.Age = int(reader.ReadInt32("Age"))
	e.Active = reader.ReadBool("Active")
}

func (e *employee) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("Name", e.Name)
	writer.WriteInt32("Age", int32(e.Age))
	writer.WriteBool("Active", e.Active)
}

func (e *employee) FactoryID() int32 {
	return 1
}

func (e *employee) ClassID() int32 {
	return 1
}

type portableFactory struct {
}

func (p portableFactory) Create(classID int32) serialization.Portable {
	return &employee{}
}

func (p portableFactory) FactoryID() int32 {
	return 1
}
