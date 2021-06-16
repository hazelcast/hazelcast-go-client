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
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type NilSerializer struct{}

func (*NilSerializer) ID() int32 {
	return ConstantTypeNil
}

func (*NilSerializer) Read(input serialization.DataInput) interface{} {
	return nil
}

func (*NilSerializer) Write(output serialization.DataOutput, i interface{}) {
}

type IdentifiedDataSerializableSerializer struct {
	factories map[int32]serialization.IdentifiedDataSerializableFactory
}

func NewIdentifiedDataSerializableSerializer(
	factories map[int32]serialization.IdentifiedDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func (*IdentifiedDataSerializableSerializer) ID() int32 {
	return ConstantTypeDataSerializable
}

func (idss *IdentifiedDataSerializableSerializer) Read(input serialization.DataInput) interface{} {
	isIdentified := input.ReadBool()
	if !isIdentified {
		err := hzerrors.NewHazelcastSerializationError("native clients do not support DataSerializable, please use IdentifiedDataSerializable", nil)
		panic(err)
	}
	factoryID := input.ReadInt32()
	classID := input.ReadInt32()
	factory := idss.factories[factoryID]
	if factory == nil {
		err := hzerrors.NewHazelcastSerializationError(fmt.Sprintf("there is no IdentifiedDataSerializable factory with ID: %d", factoryID), nil)
		panic(err)
	}
	object := factory.Create(classID)
	if object == nil {
		err := hzerrors.NewHazelcastSerializationError(fmt.Sprintf("%v is not able to create an instance for ID: %v on factory ID: %v", reflect.TypeOf(factory), classID, factoryID), nil)
		panic(err)
	}
	object.ReadData(input)
	return object
}

func (*IdentifiedDataSerializableSerializer) Write(output serialization.DataOutput, i interface{}) {
	r := i.(serialization.IdentifiedDataSerializable)
	output.WriteBool(true)
	output.WriteInt32(r.FactoryID())
	output.WriteInt32(r.ClassID())
	r.WriteData(output)
}

type ByteSerializer struct{}

func (*ByteSerializer) ID() int32 {
	return ConstantTypeByte
}

func (*ByteSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadByte()
}

func (*ByteSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteByte(i.(byte))
}

type BoolSerializer struct{}

func (*BoolSerializer) ID() int32 {
	return ConstantTypeBool
}

func (*BoolSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadBool()
}

func (*BoolSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteBool(i.(bool))
}

type UInteger16Serializer struct{}

func (*UInteger16Serializer) ID() int32 {
	return ConstantTypeUInteger16
}

func (*UInteger16Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadUInt16()
}

func (*UInteger16Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteUInt16(i.(uint16))
}

type Integer16Serializer struct{}

func (*Integer16Serializer) ID() int32 {
	return ConstantTypeInteger16
}

func (*Integer16Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt16()
}

func (*Integer16Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt16(i.(int16))
}

type Integer32Serializer struct{}

func (*Integer32Serializer) ID() int32 {
	return ConstantTypeInteger32
}

func (*Integer32Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt32()
}

func (*Integer32Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt32(i.(int32))
}

type Integer64Serializer struct{}

func (*Integer64Serializer) ID() int32 {
	return ConstantTypeInteger64
}

func (*Integer64Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt64()
}

func (*Integer64Serializer) Write(output serialization.DataOutput, i interface{}) {
	val, ok := i.(int64)
	if !ok {
		val = int64(i.(int))
	}
	output.WriteInt64(val)
}

type Float32Serializer struct{}

func (*Float32Serializer) ID() int32 {
	return ConstantTypeFloat32
}

func (*Float32Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat32()
}

func (*Float32Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat32(i.(float32))
}

type Float64Serializer struct{}

func (*Float64Serializer) ID() int32 {
	return ConstantTypeFloat64
}

func (*Float64Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat64()
}

func (*Float64Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat64(i.(float64))
}

type StringSerializer struct{}

func (*StringSerializer) ID() int32 {
	return ConstantTypeString
}

func (*StringSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadString()
}

func (*StringSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteString(i.(string))
}

type ByteArraySerializer struct{}

func (*ByteArraySerializer) ID() int32 {
	return ConstantTypeByteArray
}

func (*ByteArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadByteArray()
}

func (*ByteArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteByteArray(i.([]byte))
}

type BoolArraySerializer struct{}

func (*BoolArraySerializer) ID() int32 {
	return ConstantTypeBoolArray
}

func (*BoolArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadBoolArray()
}

func (*BoolArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteBoolArray(i.([]bool))
}

type UInteger16ArraySerializer struct{}

func (*UInteger16ArraySerializer) ID() int32 {
	return ConstantTypeUInteger16Array
}

func (*UInteger16ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadUInt16Array()

}

func (*UInteger16ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteUInt16Array(i.([]uint16))
}

type Integer16ArraySerializer struct{}

func (*Integer16ArraySerializer) ID() int32 {
	return ConstantTypeInteger16Array
}

func (*Integer16ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt16Array()
}

func (*Integer16ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt16Array(i.([]int16))
}

type Integer32ArraySerializer struct{}

func (*Integer32ArraySerializer) ID() int32 {
	return ConstantTypeInteger32Array
}

func (*Integer32ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt32Array()
}

func (*Integer32ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt32Array(i.([]int32))
}

type Integer64ArraySerializer struct{}

func (*Integer64ArraySerializer) ID() int32 {
	return ConstantTypeInteger64Array
}

func (*Integer64ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt64Array()
}

func (*Integer64ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
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
}

type Float32ArraySerializer struct{}

func (*Float32ArraySerializer) ID() int32 {
	return ConstantTypeFloat32Array
}

func (*Float32ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat32Array()
}

func (*Float32ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat32Array(i.([]float32))
}

type Float64ArraySerializer struct{}

func (*Float64ArraySerializer) ID() int32 {
	return ConstantTypeFloat64Array
}

func (*Float64ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat64Array()
}

func (*Float64ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat64Array(i.([]float64))
}

type StringArraySerializer struct{}

func (*StringArraySerializer) ID() int32 {
	return ConstantTypeStringArray
}

func (*StringArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadStringArray()
}

func (*StringArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteStringArray(i.([]string))
}

type UUIDSerializer struct{}

func (*UUIDSerializer) ID() int32 {
	return ConstantTypeUUID
}

func (*UUIDSerializer) Read(input serialization.DataInput) interface{} {
	return types.NewUUIDWith(uint64(input.ReadInt64()), uint64(input.ReadInt64()))
}

func (*UUIDSerializer) Write(output serialization.DataOutput, i interface{}) {
	uuid := i.(types.UUID)
	output.WriteInt64(int64(uuid.LeastSignificantBits()))
	output.WriteInt64(int64(uuid.MostSignificantBits()))
}

type GobSerializer struct{}

func (*GobSerializer) ID() int32 {
	return GoGobSerializationType
}

func (*GobSerializer) Read(input serialization.DataInput) interface{} {
	var network bytes.Buffer
	data := input.ReadByteArray()
	network.Write(data)
	dec := gob.NewDecoder(&network)
	var result interface{}
	if err := dec.Decode(&result); err != nil {
		panic(err)
	}
	return result
}

func (*GobSerializer) Write(output serialization.DataOutput, i interface{}) {
	var network bytes.Buffer
	t := reflect.TypeOf(i)
	v := reflect.New(t).Elem().Interface()
	// TODO: Do not auto-register types!
	gob.Register(v)
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(&i); err != nil {
		panic(fmt.Errorf("err encoding gob: %w", err))
	}
	output.WriteByteArray(network.Bytes())
}

type JSONValueSerializer struct {
}

func (js JSONValueSerializer) ID() int32 {
	return JSONSerializationType
}

func (js JSONValueSerializer) Read(input serialization.DataInput) interface{} {
	text := input.ReadString()
	return serialization.JSON(text)
}

func (js JSONValueSerializer) Write(output serialization.DataOutput, object interface{}) {
	output.WriteString(string(object.(serialization.JSON)))
}
