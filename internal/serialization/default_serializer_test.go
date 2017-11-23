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
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/serialization"
	"reflect"
	"testing"
)

func TestInteger32Serializer_Write(t *testing.T) {
	i := Integer32Serializer{}
	o := NewObjectDataOutput(9, &SerializationService{}, false)
	var a int32 = 5
	var expectedRet int32 = 7
	i.Write(o, a)
	i.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 4, &SerializationService{}, false)
	ret, _ := i.Read(in)

	if ret != expectedRet {
		t.Errorf("ToData() returns ", ret, " expected ", expectedRet)
	}
}

type factory struct{}

func (factory) Create(classId int32) IdentifiedDataSerializable {
	if classId == 1 {
		return &employee{}
	} else {
		return nil
	}
}

type employee struct {
	age  int32
	name string
}

func (e *employee) ReadData(input DataInput) error {
	e.age, _ = input.ReadInt32()
	e.name, _ = input.ReadUTF()
	return nil
}

func (e *employee) WriteData(output DataOutput) error {
	output.WriteInt32(e.age)
	output.WriteUTF(e.name)
	return nil
}

func (*employee) FactoryId() int32 {
	return 4
}

func (*employee) ClassId() int32 {
	return 1
}

func TestIdentifiedDataSerializableSerializer_Write(t *testing.T) {
	var employee1 employee = employee{22, "Furkan Şenharputlu"}
	c := NewSerializationConfig()
	c.AddDataSerializableFactory(employee1.FactoryId(), factory{})

	service := NewSerializationService(c)

	data, _ := service.ToData(&employee1)
	ret_employee, _ := service.ToObject(data)

	if !reflect.DeepEqual(employee1, *ret_employee.(*employee)) {
		t.Errorf("IdentifiedDataSerializable() works wrong!")
	}
}
