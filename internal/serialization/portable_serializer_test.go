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

package serialization

import (
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type portableFactory1 struct {
}

func (portableFactory1) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &student{}
	} else if classID == 2 {
		return &fake{}
	}
	return nil
}

func (portableFactory1) FactoryID() int32 {
	return 2
}

type portableFactory2 struct {
}

func (portableFactory2) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &student2{}
	}
	return nil
}

func (portableFactory2) FactoryID() int32 {
	return 2
}

type student struct {
	name string
	age  int32
	id   int16
}

func (*student) FactoryID() int32 {
	return 2
}

func (*student) ClassID() int32 {
	return 1
}

func (s *student) WritePortable(writer serialization.PortableWriter) {
	writer.WriteInt16("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteString("name", s.name)
}

func (s *student) ReadPortable(reader serialization.PortableReader) {
	s.id = reader.ReadInt16("id")
	s.age = reader.ReadInt32("age")
	s.name = reader.ReadString("name")
}

type student2 struct {
	name string
	id   int32
	age  int32
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

func (s *student2) WritePortable(writer serialization.PortableWriter) {
	writer.WriteInt32("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteString("name", s.name)
}

func (s *student2) ReadPortable(reader serialization.PortableReader) {
	s.id = reader.ReadInt32("id")
	s.age = reader.ReadInt32("age")
	s.name = reader.ReadString("name")
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

func (s *student3) WritePortable(writer serialization.PortableWriter) {
}

func (s *student3) ReadPortable(reader serialization.PortableReader) {
}

func TestPortableSerializer(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	expectedRet := &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)
	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}
}

func TestPortableSerializer_NoFactory(t *testing.T) {
	config := &serialization.Config{}
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	expectedRet := &student3{}
	data, err := service.ToData(expectedRet)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ToObject(data)
	if !errors.Is(err, hzerrors.ErrHazelcastSerialization) {
		t.Errorf("PortableSerializer Read() should return '%v'", fmt.Sprintf("there is no suitable portable factory for %v", 1))
	}
}

func TestPortableSerializerDuplicateFactory(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	config.SetPortableFactories(&portableFactory1{})
	if _, err := NewService(config); err == nil {
		t.Fatalf("should have failed")
	}
}

func TestPortableSerializer_NoInstanceCreated(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	expectedRet := &student3{}
	data, _ := service.ToData(expectedRet)
	_, err = service.ToObject(data)
	if !errors.Is(err, hzerrors.ErrHazelcastSerialization) {
		t.Errorf("err should be 'factory is not able to create an instance for id: 3 on factory id: 2'")
	}
}

func TestPortableSerializer_NilPortable(t *testing.T) {
	config := &serialization.Config{}
	service, _ := NewService(config)
	expectedRet := &student2{}
	data, _ := service.ToData(expectedRet)
	_, err := service.ToObject(data)

	if !errors.Is(err, hzerrors.ErrHazelcastSerialization) {
		t.Errorf("PortableSerializer Read() should return '%v'", fmt.Sprintf("there is no suitable portable factory for %v", 1))
	}
}

type fake struct {
	date        time.Time
	portable    serialization.Portable
	nilDate     *time.Time
	dec         types.Decimal
	utf         string
	utfArr      []string
	i64Arr      []int64
	i16Arr      []int16
	ui16Arr     []uint16
	f64Arr      []float64
	f32Arr      []float32
	bytArr      []byte
	i32Arr      []int32
	boolArr     []bool
	portableArr []serialization.Portable
	i64         int64
	f64         float64
	f32         float32
	i32         int32
	i16         int16
	ui16        uint16
	boo         bool
	byt         byte
}

func (*fake) FactoryID() int32 {
	return 2
}

func (*fake) ClassID() int32 {
	return 2
}

func (f *fake) WritePortable(writer serialization.PortableWriter) {
	writer.WriteByte("byt", f.byt)
	writer.WriteBool("boo", f.boo)
	writer.WriteUInt16("ui16", f.ui16)
	writer.WriteInt16("i16", f.i16)
	writer.WriteInt32("i32", f.i32)
	writer.WriteInt64("i64", f.i64)
	writer.WriteFloat32("f32", f.f32)
	writer.WriteFloat64("f64", f.f64)
	writer.WriteString("utf", f.utf)
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
	writer.WriteStringArray("utfArr", f.utfArr)
	writer.WritePortableArray("portableArr", f.portableArr)
	writer.WriteDate("nilDate", f.nilDate)
	writer.WriteDate("date", &f.date)
	writer.WriteDecimal("decimal", &f.dec)
}

func (f *fake) ReadPortable(reader serialization.PortableReader) {
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
	f.nilDate = reader.ReadDate("nilDate")
	f.date = *reader.ReadDate("date")
	f.dec = *reader.ReadDecimal("decimal")
}

func TestPortableSerializer2(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	var byt byte = 255
	var boo = true
	var ui16 uint16 = 65535
	var i16 int16 = -32768
	var i32 int32 = -2147483648
	var i64 int64 = -9223372036854775808
	var f32 float32 = -3.4e+38
	var f64 = -1.7e+308
	var utf = "Günaydın, こんにちは"
	var portable serialization.Portable = &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	var bytArr = []byte{127, 128, 255, 0, 4, 6, 8, 121}
	var boolArr = []bool{true, true, false, true, false, false, false, true, false, true}
	var ui16Arr = []uint16{65535, 65535, 65535, 1234, 23524, 13131, 9999}
	var i16Arr = []int16{-32768, -2222, 32767, 0}
	var i32Arr = []int32{-2147483648, 234123, 13123, 13144, 14134, 2147483647}
	var i64Arr = []int64{-9223372036854775808, 1231231231231, 315253647, 255225, 9223372036854775807}
	var f32Arr = []float32{-3.4e+38, 12.344, 21.2646, 3.4e+38}
	var f64Arr = []float64{-1.7e+308, 1213.2342, 45345.9887, 1.7e+308}
	var utfArr = []string{"こんにちは", "ilköğretim", "FISTIKÇIŞAHAP"}
	var portableArr = []serialization.Portable{
		&student{id: 10, age: 22, name: "Furkan Şenharputlu"},
		&student{id: 2, age: 20, name: "Micheal Micheal"},
	}
	expectedRet := &fake{
		byt: byt, boo: boo, ui16: ui16, i16: i16, i32: i32, i64: i64, f32: f32, f64: f64, utf: utf, portable: portable,
		bytArr: bytArr, boolArr: boolArr, ui16Arr: ui16Arr, i16Arr: i16Arr, i32Arr: i32Arr, i64Arr: i64Arr, f32Arr: f32Arr, f64Arr: f64Arr, utfArr: utfArr, portableArr: portableArr,
		date: time.Date(2021, 12, 6, 0, 0, 0, 0, time.Local),
		dec:  types.NewDecimal(big.NewInt(123_456_789), 100),
	}
	data, err := service.ToData(expectedRet)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedRet, ret)
}

func TestPortableSerializer3(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	service, _ := NewService(config)
	service2, _ := NewService(config)
	expectedRet := &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	data, _ := service.ToData(expectedRet)
	ret, _ := service2.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}
}

func TestPortableSerializer4(t *testing.T) {
	config1 := &serialization.Config{}
	config1.SetPortableFactories(&portableFactory1{})
	def := serialization.NewClassDefinition(2, 1, 0)
	def.AddInt16Field("id")
	def.AddInt32Field("age")
	def.AddStringField("name")
	config1.SetClassDefinitions(def)
	service, err := NewService(config1)
	if err != nil {
		t.Fatal(err)
	}
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
	var portableArr = []serialization.Portable{
		&student{id: 10, age: 22, name: "Furkan Şenharputlu"},
		&student{id: 2, age: 20, name: "Micheal Micheal"},
	}
	bint := new(big.Int)
	bint.SetString("-12346578912345678912345674891234567891346798", 10)
	expectedRet := &fake{
		byt: byt, boo: boo, ui16: ui16, i16: i16, i32: i32, i64: i64, f32: f32, f64: f64, utf: utf,
		bytArr: bytArr, boolArr: boolArr, ui16Arr: ui16Arr, i16Arr: i16Arr, i32Arr: i32Arr, i64Arr: i64Arr, f32Arr: f32Arr, f64Arr: f64Arr, utfArr: utfArr, portableArr: portableArr,
		date: time.Date(2021, 12, 6, 0, 0, 0, 0, time.Local),
		dec:  types.NewDecimal(bint, 5),
	}
	data, err := service.ToData(expectedRet)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectedRet, ret)
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

func (c *child) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("name", c.name)
}

func (c *child) ReadPortable(reader serialization.PortableReader) {
	c.name = reader.ReadString("name")
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

func (p *parent) WritePortable(writer serialization.PortableWriter) {
	writer.WritePortable("child", p.child)
}

func (p *parent) ReadPortable(reader serialization.PortableReader) {
	obj := reader.ReadPortable("child")
	p.child = obj.(*child)
}

func TestPortableSerializer_NestedPortableVersion(t *testing.T) {
	sc := &serialization.Config{}
	sc.SetPortableFactories(&portableFactory{})
	sc.PortableVersion = 6
	ss1, _ := NewService(sc)
	ss2, _ := NewService(sc)

	// make sure ss2 cached class definition of child
	if _, err := ss2.ToData(&child{"Furkan"}); err != nil {
		t.Fatal(err)
	}

	// serialized parent from ss1
	p := &parent{&child{"Furkan"}}
	data, _ := ss1.ToData(p)

	// cached class definition of child and the class definition from Data coming from ss1 should be compatible
	deserializedParent, _ := ss2.ToObject(data)
	if !reflect.DeepEqual(deserializedParent, p) {
		t.Error("nested portable version is wrong")
	}

}
