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
	"bytes"
	"encoding/gob"
	"fmt"
	. "github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
)

type NilSerializer struct{}

func (*NilSerializer) Id() int32 {
	return CONSTANT_TYPE_NULL
}

func (*NilSerializer) Read(input DataInput) (interface{}, error) {
	return nil, nil
}

func (*NilSerializer) Write(output DataOutput, i interface{}) error {
	// Empty method
	return nil
}

type IdentifiedDataSerializableSerializer struct {
	factories map[int32]IdentifiedDataSerializableFactory
}

func NewIdentifiedDataSerializableSerializer(factories map[int32]IdentifiedDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func (*IdentifiedDataSerializableSerializer) Id() int32 {
	return CONSTANT_TYPE_DATA_SERIALIZABLE
}

func (idss *IdentifiedDataSerializableSerializer) Read(input DataInput) (interface{}, error) {
	isIdentified, err := input.ReadBool()
	if err != nil {
		return nil, err
	}
	if !isIdentified {
		return nil, NewHazelcastSerializationError("native clients do not support DataSerializable, please use IdentifiedDataSerializable", nil)
	}
	factoryId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	classId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}

	var factory IdentifiedDataSerializableFactory
	factory = idss.factories[factoryId]
	if factory == nil {
		return nil, NewHazelcastSerializationError(fmt.Sprintf("there is no IdentifiedDataSerializable factory with id: %d", factoryId), nil)
	}
	var object = factory.Create(classId)
	err = object.ReadData(input)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (*IdentifiedDataSerializableSerializer) Write(output DataOutput, i interface{}) error {
	r := i.(IdentifiedDataSerializable)
	output.WriteBool(true)
	output.WriteInt32(r.FactoryId())
	output.WriteInt32(r.ClassId())
	return r.WriteData(output)
}

type ByteSerializer struct{}

func (*ByteSerializer) Id() int32 {
	return CONSTANT_TYPE_BYTE
}

func (*ByteSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadByte()
}

func (*ByteSerializer) Write(output DataOutput, i interface{}) error {
	output.WriteByte(i.(byte))
	return nil
}

type BoolSerializer struct{}

func (*BoolSerializer) Id() int32 {
	return CONSTANT_TYPE_BOOL
}

func (*BoolSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadBool()
}

func (*BoolSerializer) Write(output DataOutput, i interface{}) error {
	output.WriteBool(i.(bool))
	return nil
}

type UInteger16Serializer struct{}

func (*UInteger16Serializer) Id() int32 {
	return CONSTANT_TYPE_UINTEGER16
}

func (*UInteger16Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUInt16()
}

func (*UInteger16Serializer) Write(output DataOutput, i interface{}) error {
	output.WriteUInt16(i.(uint16))
	return nil
}

type Integer16Serializer struct{}

func (*Integer16Serializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER16
}

func (*Integer16Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt16()
}

func (*Integer16Serializer) Write(output DataOutput, i interface{}) error {
	output.WriteInt16(i.(int16))
	return nil
}

type Integer32Serializer struct{}

func (*Integer32Serializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER32
}

func (*Integer32Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt32()
}

func (*Integer32Serializer) Write(output DataOutput, i interface{}) error {
	output.WriteInt32(i.(int32))
	return nil
}

type Integer64Serializer struct{}

func (*Integer64Serializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER64
}

func (*Integer64Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt64()
}

func (*Integer64Serializer) Write(output DataOutput, i interface{}) error {
	val, ok := i.(int64)
	if !ok {
		val = int64(i.(int))
	}
	output.WriteInt64(val)
	return nil
}

type Float32Serializer struct{}

func (*Float32Serializer) Id() int32 {
	return CONSTANT_TYPE_FLOAT32
}

func (*Float32Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat32()
}

func (*Float32Serializer) Write(output DataOutput, i interface{}) error {
	output.WriteFloat32(i.(float32))
	return nil
}

type Float64Serializer struct{}

func (*Float64Serializer) Id() int32 {
	return CONSTANT_TYPE_FLOAT64
}

func (*Float64Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat64()
}

func (*Float64Serializer) Write(output DataOutput, i interface{}) error {
	output.WriteFloat64(i.(float64))
	return nil
}

type StringSerializer struct{}

func (*StringSerializer) Id() int32 {
	return CONSTANT_TYPE_STRING
}

func (*StringSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUTF()
}

func (*StringSerializer) Write(output DataOutput, i interface{}) error {
	output.WriteUTF(i.(string))
	return nil
}

type ByteArraySerializer struct{}

func (*ByteArraySerializer) Id() int32 {
	return CONSTANT_TYPE_BYTE_ARRAY
}

func (*ByteArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadByteArray()
}

func (*ByteArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteByteArray(i.([]byte))
	return nil
}

type BoolArraySerializer struct{}

func (*BoolArraySerializer) Id() int32 {
	return CONSTANT_TYPE_BOOL_ARRAY
}

func (*BoolArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadBoolArray()
}

func (*BoolArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteBoolArray(i.([]bool))
	return nil
}

type UInteger16ArraySerializer struct{}

func (*UInteger16ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_UINTEGER16_ARRAY
}

func (*UInteger16ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUInt16Array()
}

func (*UInteger16ArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteUInt16Array(i.([]uint16))
	return nil
}

type Integer16ArraySerializer struct{}

func (*Integer16ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER16_ARRAY
}

func (*Integer16ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt16Array()
}

func (*Integer16ArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteInt16Array(i.([]int16))
	return nil
}

type Integer32ArraySerializer struct{}

func (*Integer32ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER32_ARRAY
}

func (*Integer32ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt32Array()
}

func (*Integer32ArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteInt32Array(i.([]int32))
	return nil
}

type Integer64ArraySerializer struct{}

func (*Integer64ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_INTEGER64_ARRAY
}

func (*Integer64ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt64Array()
}

func (*Integer64ArraySerializer) Write(output DataOutput, i interface{}) error {
	val, ok := i.([]int64)
	if !ok {
		tmp := i.([]int)
		length := len(tmp)
		val = make([]int64, length)
		for k := 0; k < length; k++ {
			val[k] = int64(tmp[k])
		}
	}
	output.WriteInt64Array(val)
	return nil
}

type Float32ArraySerializer struct{}

func (*Float32ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_FLOAT32_ARRAY
}

func (*Float32ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat32Array()
}

func (*Float32ArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteFloat32Array(i.([]float32))
	return nil
}

type Float64ArraySerializer struct{}

func (*Float64ArraySerializer) Id() int32 {
	return CONSTANT_TYPE_FLOAT64_ARRAY
}

func (*Float64ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat64Array()
}

func (*Float64ArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteFloat64Array(i.([]float64))
	return nil
}

type StringArraySerializer struct{}

func (*StringArraySerializer) Id() int32 {
	return CONSTANT_TYPE_STRING_ARRAY
}

func (*StringArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUTFArray()
}

func (*StringArraySerializer) Write(output DataOutput, i interface{}) error {
	output.WriteUTFArray(i.([]string))
	return nil
}

type GobSerializer struct{}

func (*GobSerializer) Id() int32 {
	return GO_GOB_SERIALIZATION_TYPE
}

func (*GobSerializer) Read(input DataInput) (interface{}, error) {
	var network bytes.Buffer
	data, err := input.ReadData()
	if err != nil {
		return nil, err
	}
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	var result interface{}
	err = dec.Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (*GobSerializer) Write(output DataOutput, i interface{}) error {
	var network bytes.Buffer
	t := reflect.TypeOf(i)
	v := reflect.New(t).Elem().Interface()
	gob.Register(v)
	enc := gob.NewEncoder(&network)
	err := enc.Encode(&i)
	if err != nil {
		return err
	}
	output.WriteData(&Data{network.Bytes()})
	return nil
}
