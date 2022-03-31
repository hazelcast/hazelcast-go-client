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
		config.Serialization.SetPortableFactories(&portableFactory{})
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

type StubPortable1 struct {
	name string
	code int32
}

func (StubPortable1) FactoryID() (factoryID int32) {
	return 1
}

func (StubPortable1) ClassID() (classID int32) {
	return 1
}

func (p StubPortable1) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("name", p.name)
	writer.WriteInt32("code", p.code)
}

func (p *StubPortable1) ReadPortable(reader serialization.PortableReader) {
	p.name = reader.ReadString("name")
	p.code = reader.ReadInt32("code")
}

type StubPortable2 struct {
	name string
	code int32
}

func (StubPortable2) FactoryID() (factoryID int32) {
	return 2
}

func (StubPortable2) ClassID() (classID int32) {
	return 1
}

func (p StubPortable2) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("name", p.name)
	writer.WriteInt32("code", p.code)
}

func (p *StubPortable2) ReadPortable(reader serialization.PortableReader) {
	p.name = reader.ReadString("name")
	p.code = reader.ReadInt32("code")
}

type portableFactory2 struct {
}

func (*portableFactory2) Create(classID int32) (instance serialization.Portable) {
	if classID == 1 {
		return &StubPortable1{}
	}
	return
}

func (*portableFactory2) FactoryID() int32 {
	return 1
}

type portableFactory3 struct {
}

func (*portableFactory3) Create(classID int32) (instance serialization.Portable) {
	if classID == 1 {
		return &StubPortable2{}
	}
	return
}

func (*portableFactory3) FactoryID() int32 {
	return 2
}

func TestExplicitlyRegisteringClassDefinition_MultiplePortableFactories(t *testing.T) {
	addFields := func(cd *serialization.ClassDefinition) {
		cd.AddStringField("name")
		cd.AddInt32Field("code")
	}
	it.MapTesterWithConfig(t, func(config *hz.Config) {
		commonClassID := int32(1)
		fcFirst := &portableFactory2{}
		fcSecond := &portableFactory3{}
		config.Serialization.SetPortableFactories(fcFirst, fcSecond)
		stubPortable1CD := serialization.NewClassDefinition(fcFirst.FactoryID(), commonClassID, 1)
		addFields(stubPortable1CD)
		stubPortable2CD := serialization.NewClassDefinition(fcSecond.FactoryID(), commonClassID, 1)
		addFields(stubPortable2CD)
	}, func(t *testing.T, m *hz.Map) {
		t.Run("set StubPortable1 to map", func(t *testing.T) {
			sb1 := &StubPortable1{name: "sb1Name", code: 1}
			it.Must(m.Set(context.Background(), "sb1", sb1))
			if value, err := m.Get(context.Background(), "sb1"); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(sb1, value) {
				t.Fatalf("target %#v: %#v", sb1, value)
			}
		})
		t.Run("set StubPortable2 to map", func(t *testing.T) {
			sb2 := &StubPortable2{name: "sb2Name", code: 2}
			it.Must(m.Set(context.Background(), "sb2", sb2))
			if value, err := m.Get(context.Background(), "sb2"); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(sb2, value) {
				t.Fatalf("target %#v: %#v", sb2, value)
			}
		})
	})
}
