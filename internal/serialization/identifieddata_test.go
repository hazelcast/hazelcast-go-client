/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const employeeClassID = int32(1)
const employeeFactoryID = int32(4)

type employee struct {
	name string
	age  int32
}

func (*employee) FactoryID() int32 {
	return employeeFactoryID
}

func (*employee) ClassID() int32 {
	return employeeClassID
}

type factory struct{}

func (factory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == employeeClassID {
		return &employee{}
	}
	return nil
}

func (factory) FactoryID() int32 {
	return employeeFactoryID
}

type nullFactory struct{}

func (nullFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	return nil
}

func (nullFactory) FactoryID() int32 {
	return employeeFactoryID
}

func (e *employee) ReadData(input serialization.DataInput) {
	e.age = input.ReadInt32()
	e.name = input.ReadString()
}

func (e *employee) WriteData(output serialization.DataOutput) {
	output.WriteInt32(e.age)
	output.WriteString(e.name)
}

func TestIdentifiedDataSerializableSerializer_Write(t *testing.T) {
	var employee1 = &employee{age: 22, name: "Furkan Şenharputlu"}
	c := &serialization.Config{}
	c.SetIdentifiedDataSerializableFactories(&factory{})
	service, err := iserialization.NewService(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := service.ToData(employee1)
	if err != nil {
		t.Fatal(err)
	}
	retEmployee, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, employee1, retEmployee)
}

func TestIdentifiedDataSerializableSerializer_NoInstanceCreated(t *testing.T) {
	e := &employee{age: 38, name: "Jack"}
	c := &serialization.Config{}
	c.SetIdentifiedDataSerializableFactories(&nullFactory{})
	service, err := iserialization.NewService(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := service.ToData(e)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ToObject(data)
	if !errors.Is(err, hzerrors.ErrHazelcastSerialization) {
		t.Fatalf("should fail as HazelcastSerializationError")
	}
}
