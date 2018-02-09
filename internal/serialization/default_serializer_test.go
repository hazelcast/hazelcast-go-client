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

package serialization

import (
	"fmt"
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"testing"
)

func TestNilSerializer_Write(t *testing.T) {
	service := &SerializationService{}
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
		t.Errorf("IdentifiedDataSerializableSerializer failed")
	}
}

func TestIdentifiedDataSerializableSerializer_DataSerializable(t *testing.T) {
	serializer := &IdentifiedDataSerializableSerializer{}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteBool(false)
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	_, err := serializer.Read(in)

	if _, ok := err.(*core.HazelcastSerializationError); !ok {
		t.Errorf("IdentifiedDataSerializableSerializer Read() should return 'native clients do not support DataSerializable, please use IdentifiedDataSerializable'")
	}
}

type x struct {
}

func (*x) FactoryId() int32 {
	return 1
}

func (*x) ClassId() int32 {
	return 1
}

func (*x) WriteData(output DataOutput) error {
	return nil
}

func (*x) ReadData(input DataInput) error {
	return nil
}

func TestIdentifiedDataSerializableSerializer_NoFactory(t *testing.T) {
	serializer := &IdentifiedDataSerializableSerializer{}
	o := NewObjectDataOutput(0, nil, false)
	serializer.Write(o, &x{})
	in := NewObjectDataInput(o.buffer, 0, nil, false)
	_, err := serializer.Read(in)

	if _, ok := err.(*core.HazelcastSerializationError); !ok {
		t.Errorf("IdentifiedDataSerializableSerializer Read() should return '%v'", fmt.Sprintf("there is no IdentifiedDataSerializable factory with id: %d", 1))
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
		t.Errorf("UInteger16Serializer failed")
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
		t.Errorf("Integer16Serializer failed")
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
		t.Errorf("Integer32Serializer failed")
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
		t.Errorf("Integer64Serializer failed")
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
		t.Errorf("Float32Serializer failed")
	}
}

func TestFloat64Serializer_Write(t *testing.T) {
	serializer := Float64Serializer{}
	o := NewObjectDataOutput(9, nil, false)
	var a float64 = 5.234234
	var expectedRet float64 = 7.123234
	serializer.Write(o, a)
	serializer.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 8, nil, false)
	ret, _ := serializer.Read(in)

	if ret != expectedRet {
		t.Errorf("Float64Serializer failed")
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
		t.Errorf("ByteArraySerializer failed")
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
		t.Errorf("BoolArraySerializer failed")
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
		t.Errorf("UInteger16ArraySerializer failed")
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
		t.Errorf("StringArraySerializer failed")
	}
}
