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
	"time"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type NilSerializer struct{}

func (NilSerializer) ID() int32 {
	return TypeNil
}

func (NilSerializer) Read(input serialization.DataInput) interface{} {
	return nil
}

func (NilSerializer) Write(output serialization.DataOutput, i interface{}) {
}

type IdentifiedDataSerializableSerializer struct {
	factories map[int32]serialization.IdentifiedDataSerializableFactory
}

func NewIdentifiedDataSerializableSerializer(
	factories map[int32]serialization.IdentifiedDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func (IdentifiedDataSerializableSerializer) ID() int32 {
	return TypeDataSerializable
}

func (idss IdentifiedDataSerializableSerializer) Read(input serialization.DataInput) interface{} {
	isIdentified := input.ReadBool()
	if !isIdentified {
		err := ihzerrors.NewSerializationError("native clients do not support DataSerializable, please use IdentifiedDataSerializable", nil)
		panic(err)
	}
	factoryID := input.ReadInt32()
	classID := input.ReadInt32()
	factory := idss.factories[factoryID]
	if factory == nil {
		err := ihzerrors.NewSerializationError(fmt.Sprintf("there is no IdentifiedDataSerializable factory with ID: %d", factoryID), nil)
		panic(err)
	}
	object := factory.Create(classID)
	if object == nil {
		err := ihzerrors.NewSerializationError(fmt.Sprintf("%v is not able to create an instance for ID: %v on factory ID: %v", reflect.TypeOf(factory), classID, factoryID), nil)
		panic(err)
	}
	object.ReadData(input)
	return object
}

func (IdentifiedDataSerializableSerializer) Write(output serialization.DataOutput, i interface{}) {
	r := i.(serialization.IdentifiedDataSerializable)
	output.WriteBool(true)
	output.WriteInt32(r.FactoryID())
	output.WriteInt32(r.ClassID())
	r.WriteData(output)
}

type ByteSerializer struct{}

func (ByteSerializer) ID() int32 {
	return TypeByte
}

func (ByteSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadByte()
}

func (ByteSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteByte(i.(byte))
}

type BoolSerializer struct{}

func (BoolSerializer) ID() int32 {
	return TypeBool
}

func (BoolSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadBool()
}

func (BoolSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteBool(i.(bool))
}

type UInt16Serializer struct{}

func (UInt16Serializer) ID() int32 {
	return TypeUInt16
}

func (UInt16Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadUInt16()
}

func (UInt16Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteUInt16(i.(uint16))
}

type IntSerializer struct{}

func (IntSerializer) ID() int32 {
	return TypeInt64
}

func (IntSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt64()
}

func (IntSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt64(int64(i.(int)))
}

type Int16Serializer struct{}

func (Int16Serializer) ID() int32 {
	return TypeInt16
}

func (Int16Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt16()
}

func (Int16Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt16(i.(int16))
}

type Int32Serializer struct{}

func (Int32Serializer) ID() int32 {
	return TypeInt32
}

func (Int32Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt32()
}

func (Int32Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt32(i.(int32))
}

type Int64Serializer struct{}

func (Int64Serializer) ID() int32 {
	return TypeInt64
}

func (Int64Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt64()
}

func (Int64Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt64(i.(int64))
}

type Float32Serializer struct{}

func (Float32Serializer) ID() int32 {
	return TypeFloat32
}

func (Float32Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat32()
}

func (Float32Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat32(i.(float32))
}

type Float64Serializer struct{}

func (Float64Serializer) ID() int32 {
	return TypeFloat64
}

func (Float64Serializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat64()
}

func (Float64Serializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat64(i.(float64))
}

type StringSerializer struct{}

func (StringSerializer) ID() int32 {
	return TypeString
}

func (StringSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadString()
}

func (StringSerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteString(i.(string))
}

type ByteArraySerializer struct{}

func (ByteArraySerializer) ID() int32 {
	return TypeByteArray
}

func (ByteArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadByteArray()
}

func (ByteArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteByteArray(i.([]byte))
}

type BoolArraySerializer struct{}

func (BoolArraySerializer) ID() int32 {
	return TypeBoolArray
}

func (BoolArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadBoolArray()
}

func (BoolArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteBoolArray(i.([]bool))
}

type UInt16ArraySerializer struct{}

func (UInt16ArraySerializer) ID() int32 {
	return TypeUInt16Array
}

func (UInt16ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadUInt16Array()

}

func (UInt16ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteUInt16Array(i.([]uint16))
}

type Int16ArraySerializer struct{}

func (Int16ArraySerializer) ID() int32 {
	return TypeInt16Array
}

func (Int16ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt16Array()
}

func (Int16ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt16Array(i.([]int16))
}

type Int32ArraySerializer struct{}

func (Int32ArraySerializer) ID() int32 {
	return TypeInt32Array
}

func (Int32ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt32Array()
}

func (Int32ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteInt32Array(i.([]int32))
}

type Int64ArraySerializer struct{}

func (Int64ArraySerializer) ID() int32 {
	return TypeInt64Array
}

func (Int64ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadInt64Array()
}

func (Int64ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
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

func (Float32ArraySerializer) ID() int32 {
	return TypeFloat32Array
}

func (Float32ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat32Array()
}

func (Float32ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat32Array(i.([]float32))
}

type Float64ArraySerializer struct{}

func (Float64ArraySerializer) ID() int32 {
	return TypeFloat64Array
}

func (Float64ArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadFloat64Array()
}

func (Float64ArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteFloat64Array(i.([]float64))
}

type StringArraySerializer struct{}

func (StringArraySerializer) ID() int32 {
	return TypeStringArray
}

func (StringArraySerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadStringArray()
}

func (StringArraySerializer) Write(output serialization.DataOutput, i interface{}) {
	output.WriteStringArray(i.([]string))
}

type UUIDSerializer struct{}

func (UUIDSerializer) ID() int32 {
	return TypeUUID
}

func (UUIDSerializer) Read(input serialization.DataInput) interface{} {
	return types.NewUUIDWith(uint64(input.ReadInt64()), uint64(input.ReadInt64()))
}

func (UUIDSerializer) Write(output serialization.DataOutput, i interface{}) {
	uuid := i.(types.UUID)
	output.WriteInt64(int64(uuid.LeastSignificantBits()))
	output.WriteInt64(int64(uuid.MostSignificantBits()))
}

type JavaDateSerializer struct{}

func (JavaDateSerializer) ID() int32 {
	return TypeJavaDate
}

func (JavaDateSerializer) Read(input serialization.DataInput) interface{} {
	return time.Unix(0, input.ReadInt64()*1_000)
}

func (JavaDateSerializer) Write(output serialization.DataOutput, i interface{}) {
	t := i.(time.Time)
	output.WriteInt64(t.UnixNano() / 1000)
}

type JavaClassSerializer struct{}

func (JavaClassSerializer) ID() int32 {
	return TypeJavaClass
}

func (JavaClassSerializer) Read(input serialization.DataInput) interface{} {
	return input.ReadString()
}

func (JavaClassSerializer) Write(output serialization.DataOutput, i interface{}) {
	// no-op
}

type JavaLinkedListSerializer struct{}

func (JavaLinkedListSerializer) ID() int32 {
	return TypeJavaLinkedList
}

func (JavaLinkedListSerializer) Read(input serialization.DataInput) interface{} {
	count := int(input.ReadInt32())
	res := make([]interface{}, count)
	for i := 0; i < count; i++ {
		res[i] = input.ReadObject()
	}
	return res
}

func (JavaLinkedListSerializer) Write(output serialization.DataOutput, i interface{}) {
	// no-op
}

type JavaArrayListSerializer struct{}

func (JavaArrayListSerializer) ID() int32 {
	return TypeJavaArrayList
}

func (JavaArrayListSerializer) Read(input serialization.DataInput) interface{} {
	count := int(input.ReadInt32())
	res := make([]interface{}, count)
	for i := 0; i < count; i++ {
		res[i] = input.ReadObject()
	}
	return res
}

func (JavaArrayListSerializer) Write(output serialization.DataOutput, i interface{}) {
	// no-op
}

type GobSerializer struct{}

func (GobSerializer) ID() int32 {
	return TypeGobSerialization
}

func (GobSerializer) Read(input serialization.DataInput) interface{} {
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

func (GobSerializer) Write(output serialization.DataOutput, i interface{}) {
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
	return TypeJSONSerialization
}

func (js JSONValueSerializer) Read(input serialization.DataInput) interface{} {
	text := input.ReadString()
	return serialization.JSON(text)
}

func (js JSONValueSerializer) Write(output serialization.DataOutput, object interface{}) {
	output.WriteString(string(object.(serialization.JSON)))
}
