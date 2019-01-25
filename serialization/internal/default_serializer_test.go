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

package internal

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestNilSerializer_Write(t *testing.T) {
	service := &Service{}
	serializer := &NilSerializer{}
	o := NewObjectDataOutput(0, service, false)
	serializer.Write(o, nil)
	in := NewObjectDataInput(o.buffer, 0, service, false)
	ret, _ := serializer.Read(in)

	if ret != nil {
		t.Errorf("ToData() returns %v expected %v", ret, nil)
	}
}

type factory struct{}

func (factory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == 1 {
		return &employee{}
	}
	return nil
}

type employee struct {
	age  int32
	name string
}

func (e *employee) ReadData(input serialization.DataInput) error {
	e.age = input.ReadInt32()
	e.name = input.ReadUTF()
	return nil
}

func (e *employee) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(e.age)
	output.WriteUTF(e.name)
	return nil
}

func (*employee) FactoryID() int32 {
	return 4
}

func (*employee) ClassID() int32 {
	return 1
}

type customer struct {
	age  int32
	name string
}

func (c *customer) ReadData(input serialization.DataInput) error {
	c.age = input.ReadInt32()
	c.name = input.ReadUTF()
	return nil
}

func (c *customer) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(c.age)
	output.WriteUTF(c.name)
	return nil
}

func (*customer) FactoryID() int32 {
	return 4
}

func (*customer) ClassID() int32 {
	return 2
}

func TestIdentifiedDataSerializableSerializer_Write(t *testing.T) {
	var employee1 = employee{22, "Furkan Şenharputlu"}
	c := serialization.NewConfig()
	c.AddDataSerializableFactory(employee1.FactoryID(), factory{})

	service, _ := NewService(c)

	data, _ := service.ToData(&employee1)
	retEmployee, _ := service.ToObject(data)

	if !reflect.DeepEqual(employee1, *retEmployee.(*employee)) {
		t.Error("IdentifiedDataSerializableSerializer failed")
	}
}

func TestIdentifiedDataSerializableSerializer_NoInstanceCreated(t *testing.T) {
	c := &customer{38, "Jack"}
	config := serialization.NewConfig()
	config.AddDataSerializableFactory(c.FactoryID(), factory{})

	service, _ := NewService(config)

	data, _ := service.ToData(c)
	_, err := service.ToObject(data)

	if _, ok := err.(*core.HazelcastSerializationError); !ok {
		t.Error("err should be 'factory is not able to create an instance for id: 2 on factory id: 4'")
	}
}

func TestIdentifiedDataSerializableSerializer_DataSerializable(t *testing.T) {
	serializer := &IdentifiedDataSerializableSerializer{}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteBool(false)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	_, err := serializer.Read(in)

	if _, ok := err.(*core.HazelcastSerializationError); !ok {
		t.Error("IdentifiedDataSerializableSerializer Read() should return" +
			" 'native clients do not support DataSerializable, please use IdentifiedDataSerializable'")
	}
}

type x struct {
}

func (*x) FactoryID() int32 {
	return 1
}

func (*x) ClassID() int32 {
	return 1
}

func (*x) WriteData(output serialization.DataOutput) error {
	return nil
}

func (*x) ReadData(input serialization.DataInput) error {
	return nil
}

func TestIdentifiedDataSerializableSerializer_NoFactory(t *testing.T) {
	serializer := &IdentifiedDataSerializableSerializer{}
	o := NewObjectDataOutput(0, nil, false)
	serializer.Write(o, &x{})
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	_, err := serializer.Read(in)

	if _, ok := err.(*core.HazelcastSerializationError); !ok {
		t.Errorf("IdentifiedDataSerializableSerializer Read() should return '%v'",
			fmt.Sprintf("there is no IdentifiedDataSerializable factory with id: %d", 1))
	}
}

func TestUInteger16Serializer_Write(t *testing.T) {
	serializer := UInteger16Serializer{}
	o := NewObjectDataOutput(0, nil, false)
	var a uint16 = 5
	var expectedRet uint16 = 7
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 2, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("UInteger16Serializer failed")
	}
}

func TestInteger16Serializer_Write(t *testing.T) {
	serializer := Integer16Serializer{}
	o := NewObjectDataOutput(0, nil, false)
	var a int16 = 5
	var expectedRet int16 = 7
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 2, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("Integer16Serializer failed")
	}
}

func TestInteger32Serializer_Write(t *testing.T) {
	serializer := Integer32Serializer{}
	o := NewObjectDataOutput(9, nil, false)
	var a int32 = 5
	var expectedRet int32 = 7
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 4, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("Integer32Serializer failed")
	}
}

func TestInteger64Serializer_Write(t *testing.T) {
	serializer := Integer64Serializer{}
	o := NewObjectDataOutput(0, nil, false)
	var a int64 = 5
	var expectedRet int64 = 7
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 8, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("Integer64Serializer failed")
	}
}

func TestFloat32Serializer_Write(t *testing.T) {
	serializer := Float32Serializer{}
	o := NewObjectDataOutput(9, nil, false)
	var a float32 = 5.234
	var expectedRet float32 = 7.123
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 4, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("Float32Serializer failed")
	}
}

func TestFloat64Serializer_Write(t *testing.T) {
	serializer := Float64Serializer{}
	o := NewObjectDataOutput(9, nil, false)
	var a = 5.234234
	var expectedRet = 7.123234
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 8, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Error("Float64Serializer failed")
	}
}

func TestByteArraySerializer_Write(t *testing.T) {
	serializer := ByteArraySerializer{}
	o := NewObjectDataOutput(9, nil, false)
	var expectedRet = []byte{23, 12, 23, 43}
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	ret, _ := serializer.Read(in)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("ByteArraySerializer failed")
	}
}

func TestBoolArraySerializer_Write(t *testing.T) {
	serializer := BoolArraySerializer{}
	o := NewObjectDataOutput(9, nil, false)
	var expectedRet = []bool{true, true, true, false, true, false, false, true, false}
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	ret, _ := serializer.Read(in)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("BoolArraySerializer failed")
	}
}

func TestUInteger16ArraySerializer_Write(t *testing.T) {
	serializer := UInteger16ArraySerializer{}
	o := NewObjectDataOutput(9, nil, false)
	var expectedRet = []uint16{23, 12, 23, 43, 23, 234, 45}
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	ret, _ := serializer.Read(in)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("UInteger16ArraySerializer failed")
	}
}

func TestStringArraySerializer_Write(t *testing.T) {
	serializer := StringArraySerializer{}
	o := NewObjectDataOutput(9, nil, false)
	var expectedRet = []string{"Hello", "Merhaba", "こんにちは"}
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	ret, _ := serializer.Read(in)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("StringArraySerializer failed")
	}
}
