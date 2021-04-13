// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type portableFactory1 struct {
}

func (*portableFactory1) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &student{}
	} else if classID == 2 {
		return &fake{}
	}
	return nil
}

func (*portableFactory1) FactoryID() int32 {
	return 2
}

type portableFactory2 struct {
}

func (*portableFactory2) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &student2{}
	}
	return nil
}

type student struct {
	id   int16
	age  int32
	name string
}

func (*student) FactoryID() int32 {
	return 2
}

func (*student) ClassID() int32 {
	return 1
}

func (s *student) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteInt16("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteUTF("name", s.name)
	return nil
}

func (s *student) ReadPortable(reader serialization.PortableReader) error {
	s.id = reader.ReadInt16("id")
	s.age = reader.ReadInt32("age")
	s.name = reader.ReadString("name")
	return reader.Error()
}

type student2 struct {
	id   int32
	age  int32
	name string
}

func (*student2) FactoryID() int32 {
	return 2
}

func (*student2) ClassID() int32 {
	return 1
}

func (*student2) Version() int32 {
	return 1
}

func (s *student2) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteInt32("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteUTF("name", s.name)
	return nil
}

func (s *student2) ReadPortable(reader serialization.PortableReader) error {
	s.id = reader.ReadInt32("id")
	s.age = reader.ReadInt32("age")
	s.name = reader.ReadString("name")
	return nil
}

type student3 struct {
}

func (*student3) FactoryID() int32 {
	return 2
}

func (*student3) ClassID() int32 {
	return 3
}

func (*student3) Version() int32 {
	return 1
}

func (s *student3) WritePortable(writer serialization.PortableWriter) error {
	return nil
}

func (s *student3) ReadPortable(reader serialization.PortableReader) error {
	return nil
}

func TestPortableSerializer(t *testing.T) {
	config := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	config.PortableFactories[2] = &portableFactory1{}
	service, _ := NewService(config)
	expectedRet := &student{10, 22, "Furkan Şenharputlu"}
	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}
}

func TestPortableSerializer_NoFactory(t *testing.T) {
	config := &serialization.Config{}
	service, _ := NewService(config)
	expectedRet := &student3{}
	data, _ := service.ToData(expectedRet)
	_, err := service.ToObject(data)

	if _, ok := err.(*hzerror.HazelcastSerializationError); !ok {
		t.Errorf("PortableSerializer Read() should return '%v'", fmt.Sprintf("there is no suitable portable factory for %v", 1))
	}
}

func TestPortableSerializer_NoInstanceCreated(t *testing.T) {
	config := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	config.PortableFactories[2] = &portableFactory1{}
	service, _ := NewService(config)
	expectedRet := &student3{}
	data, _ := service.ToData(expectedRet)
	_, err := service.ToObject(data)

	if _, ok := err.(*hzerror.HazelcastSerializationError); !ok {
		fmt.Println(err)
		t.Errorf("err should be 'factory is not able to create an instance for id: 3 on factory id: 2'")
	}
}

func TestPortableSerializer_NilPortable(t *testing.T) {
	config := &serialization.Config{}
	service, _ := NewService(config)
	expectedRet := &student2{}
	data, _ := service.ToData(expectedRet)
	_, err := service.ToObject(data)

	if _, ok := err.(*hzerror.HazelcastSerializationError); !ok {
		t.Errorf("PortableSerializer Read() should return '%v'", fmt.Sprintf("there is no suitable portable factory for %v", 1))
	}
}

type fake struct {
	byt         byte
	boo         bool
	ui16        uint16
	i16         int16
	i32         int32
	i64         int64
	f32         float32
	f64         float64
	utf         string
	portable    serialization.Portable
	bytArr      []byte
	boolArr     []bool
	ui16Arr     []uint16
	i16Arr      []int16
	i32Arr      []int32
	i64Arr      []int64
	f32Arr      []float32
	f64Arr      []float64
	utfArr      []string
	portableArr []serialization.Portable
}

func (*fake) FactoryID() int32 {
	return 2
}

func (*fake) ClassID() int32 {
	return 2
}

func (f *fake) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteByte("byt", f.byt)
	writer.WriteBool("boo", f.boo)
	writer.WriteUInt16("ui16", f.ui16)
	writer.WriteInt16("i16", f.i16)
	writer.WriteInt32("i32", f.i32)
	writer.WriteInt64("i64", f.i64)
	writer.WriteFloat32("f32", f.f32)
	writer.WriteFloat64("f64", f.f64)
	writer.WriteUTF("utf", f.utf)
	if f.portable != nil {
		writer.WritePortable("portable", f.portable)
	} else {
		writer.WriteNilPortable("portable", 2, 1)
	}
	writer.WriteByteArray("bytArr", f.bytArr)
	writer.WriteBoolArray("boolArr", f.boolArr)
	writer.WriteUInt16Array("ui16Arr", f.ui16Arr)
	writer.WriteInt16Array("i16Arr", f.i16Arr)
	writer.WriteInt32Array("i32Arr", f.i32Arr)
	writer.WriteInt64Array("i64Arr", f.i64Arr)
	writer.WriteFloat32Array("f32Arr", f.f32Arr)
	writer.WriteFloat64Array("f64Arr", f.f64Arr)
	writer.WriteUTFArray("utfArr", f.utfArr)
	writer.WritePortableArray("portableArr", f.portableArr)
	return nil
}

func (f *fake) ReadPortable(reader serialization.PortableReader) error {
	f.byt = reader.ReadByte("byt")
	f.boo = reader.ReadBool("boo")
	f.ui16 = reader.ReadUInt16("ui16")
	f.i16 = reader.ReadInt16("i16")
	f.i32 = reader.ReadInt32("i32")
	f.i64 = reader.ReadInt64("i64")
	f.f32 = reader.ReadFloat32("f32")
	f.f64 = reader.ReadFloat64("f64")
	f.utf = reader.ReadString("utf")
	f.portable = reader.ReadPortable("portable")
	f.bytArr = reader.ReadByteArray("bytArr")
	f.boolArr = reader.ReadBoolArray("boolArr")
	f.ui16Arr = reader.ReadUInt16Array("ui16Arr")
	f.i16Arr = reader.ReadInt16Array("i16Arr")
	f.i32Arr = reader.ReadInt32Array("i32Arr")
	f.i64Arr = reader.ReadInt64Array("i64Arr")
	f.f32Arr = reader.ReadFloat32Array("f32Arr")
	f.f64Arr = reader.ReadFloat64Array("f64Arr")
	f.utfArr = reader.ReadStringArray("utfArr")
	f.portableArr = reader.ReadPortableArray("portableArr")
	return nil
}

func TestPortableSerializer2(t *testing.T) {
	config := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	config.PortableFactories[2] = &portableFactory1{}
	service, _ := NewService(config)

	var byt byte = 255
	var boo = true
	var ui16 uint16 = 65535
	var i16 int16 = -32768
	var i32 int32 = -2147483648
	var i64 int64 = -9223372036854775808
	var f32 float32 = -3.4e+38
	var f64 = -1.7e+308
	var utf = "Günaydın, こんにちは"
	var portable serialization.Portable = &student{10, 22, "Furkan Şenharputlu"}
	var bytArr = []byte{127, 128, 255, 0, 4, 6, 8, 121}
	var boolArr = []bool{true, true, false, true, false, false, false, true, false, true}
	var ui16Arr = []uint16{65535, 65535, 65535, 1234, 23524, 13131, 9999}
	var i16Arr = []int16{-32768, -2222, 32767, 0}
	var i32Arr = []int32{-2147483648, 234123, 13123, 13144, 14134, 2147483647}
	var i64Arr = []int64{-9223372036854775808, 1231231231231, 315253647, 255225, 9223372036854775807}
	var f32Arr = []float32{-3.4e+38, 12.344, 21.2646, 3.4e+38}
	var f64Arr = []float64{-1.7e+308, 1213.2342, 45345.9887, 1.7e+308}
	var utfArr = []string{"こんにちは", "ilköğretim", "FISTIKÇIŞAHAP"}
	var portableArr = []serialization.Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{2, 20, "Micheal Micheal"}}

	expectedRet := &fake{byt, boo, ui16, i16, i32, i64, f32, f64, utf, portable,
		bytArr, boolArr, ui16Arr, i16Arr, i32Arr, i64Arr, f32Arr, f64Arr, utfArr, portableArr}
	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("ReadObject() failed")
	}

}

func TestPortableSerializer3(t *testing.T) {
	config := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	config.PortableFactories[2] = &portableFactory1{}
	service, _ := NewService(config)
	service2, _ := NewService(config)
	expectedRet := &student{10, 22, "Furkan Şenharputlu"}
	data, _ := service.ToData(expectedRet)
	ret, _ := service2.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}
}

func TestPortableSerializer4(t *testing.T) {
	config1 := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	config1.PortableFactories[2] = &portableFactory1{}
	config2 := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	builder := NewClassDefinitionBuilder(2, 1, 0)
	err := builder.AddInt16Field("id")
	if err != nil {
		t.Errorf("ClassDefinitionBuilder works wrong")
	}
	err = builder.AddInt32Field("age")
	if err != nil {
		t.Errorf("ClassDefinitionBuilder works wrong")
	}
	err = builder.AddUTFField("name")
	if err != nil {
		t.Error("ClassDefinitionBuilder works wrong")
	}
	cd := builder.Build()
	config1.ClassDefinitions = append(config1.ClassDefinitions, cd)

	service, _ := NewService(config1)

	var byt byte = 255
	var boo = true
	var ui16 uint16 = 65535
	var i16 int16 = -32768
	var i32 int32 = -2147483648
	var i64 int64 = -9223372036854775808
	var f32 float32 = -3.4e+38
	var f64 = -1.7e+308
	var utf = "Günaydın, こんにちは"
	var bytArr = []byte{127, 128, 255, 0, 4, 6, 8, 121}
	var boolArr = []bool{true, true, false, true, false, false, false, true, false, true}
	var ui16Arr = []uint16{65535, 65535, 65535, 1234, 23524, 13131, 9999}
	var i16Arr = []int16{-32768, -2222, 32767, 0}
	var i32Arr = []int32{-2147483648, 234123, 13123, 13144, 14134, 2147483647}
	var i64Arr = []int64{-9223372036854775808, 1231231231231, 315253647, 255225, 9223372036854775807}
	var f32Arr = []float32{-3.4e+38, 12.344, 21.2646, 3.4e+38}
	var f64Arr = []float64{-1.7e+308, 1213.2342, 45345.9887, 1.7e+308}
	var utfArr = []string{"こんにちは", "ilköğretim", "FISTIKÇIŞAHAP"}
	var portableArr = []serialization.Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{2, 20, "Micheal Micheal"}}

	expectedRet := &fake{byt, boo, ui16, i16, i32, i64, f32, f64, utf, nil,
		bytArr, boolArr, ui16Arr, i16Arr, i32Arr, i64Arr, f32Arr, f64Arr, utfArr, portableArr}

	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)

	config2.PortableFactories[2] = &portableFactory1{}

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Error("ReadObject() failed")
	}
}

type portableFactory struct {
}

func (*portableFactory) Create(classID int32) (instance serialization.Portable) {
	if classID == 1 {
		return &parent{}
	} else if classID == 2 {
		return &child{}
	}
	return
}

func (*portableFactory) FactoryID() int32 {
	return 1
}

type child struct {
	name string
}

func (*child) FactoryID() (factoryID int32) {
	return 1
}

func (*child) ClassID() (classID int32) {
	return 2
}

func (c *child) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteUTF("name", c.name)
	return
}

func (c *child) ReadPortable(reader serialization.PortableReader) (err error) {
	c.name = reader.ReadString("name")
	return
}

type parent struct {
	child *child
}

func (*parent) FactoryID() (factoryID int32) {
	return 1
}

func (*parent) ClassID() (classID int32) {
	return 1
}

func (p *parent) WritePortable(writer serialization.PortableWriter) (err error) {
	return writer.WritePortable("child", p.child)
}

func (p *parent) ReadPortable(reader serialization.PortableReader) (err error) {
	obj := reader.ReadPortable("child")
	p.child = obj.(*child)
	return
}

func TestPortableSerializer_NestedPortableVersion(t *testing.T) {
	sc := &serialization.Config{PortableFactories: map[int32]serialization.PortableFactory{}}
	sc.PortableVersion = 6
	sc.PortableFactories[1] = &portableFactory{}
	ss1, _ := NewService(sc)
	ss2, _ := NewService(sc)

	// make sure ss2 cached class definition of child
	ss2.ToData(&child{"Furkan"})

	// serialized parent from ss1
	p := &parent{&child{"Furkan"}}
	data, _ := ss1.ToData(p)

	// cached class definition of child and the class definition from Data coming from ss1 should be compatible
	deserializedParent, _ := ss2.ToObject(data)
	if !reflect.DeepEqual(deserializedParent, p) {
		t.Error("nested portable version is wrong")
	}

}
