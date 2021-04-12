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

package compatibility

import (
	serialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type anIdentifiedDataSerializable struct {
	bool bool
	b    byte
	c    uint16
	d    float64
	s    int16
	f    float32
	i    int32
	l    int64
	str  string

	booleans []bool
	bytes    []byte
	chars    []uint16
	doubles  []float64
	shorts   []int16
	floats   []float32
	ints     []int32
	longs    []int64
	strings  []string

	booleansNull []bool
	bytesNull    []byte
	charsNull    []uint16
	doublesNull  []float64
	shortsNull   []int16
	floatsNull   []float32
	intsNull     []int32
	longsNull    []int64
	stringsNull  []string

	portableObject                   serialization.Portable
	identifiedDataSerializableObject serialization.IdentifiedDataSerializable
}

func (*anIdentifiedDataSerializable) ClassID() int32 {
	return dataSerializableClassID
}

func (*anIdentifiedDataSerializable) FactoryID() int32 {
	return identifiedDataSerializableFactoryID
}

func (i *anIdentifiedDataSerializable) WriteData(output serialization.DataOutput) error {
	var err error
	output.WriteBool(i.bool)
	output.WriteByte(i.b)
	output.WriteUInt16(i.c)
	output.WriteFloat64(i.d)
	output.WriteInt16(i.s)
	output.WriteFloat32(i.f)
	output.WriteInt32(i.i)
	output.WriteInt64(i.l)
	output.WriteString(i.str)

	output.WriteBoolArray(i.booleans)
	output.WriteByteArray(i.bytes)
	output.WriteUInt16Array(i.chars)
	output.WriteFloat64Array(i.doubles)
	output.WriteInt16Array(i.shorts)
	output.WriteFloat32Array(i.floats)
	output.WriteInt32Array(i.ints)
	output.WriteInt64Array(i.longs)
	output.WriteUTFArray(i.strings)

	output.WriteBoolArray(i.booleansNull)
	output.WriteByteArray(i.bytesNull)
	output.WriteUInt16Array(i.charsNull)
	output.WriteFloat64Array(i.doublesNull)
	output.WriteInt16Array(i.shortsNull)
	output.WriteFloat32Array(i.floatsNull)
	output.WriteInt32Array(i.intsNull)
	output.WriteInt64Array(i.longsNull)
	output.WriteUTFArray(i.stringsNull)

	err = output.WriteObject(i.portableObject)
	if err != nil {
		return err
	}
	err = output.WriteObject(i.identifiedDataSerializableObject)
	if err != nil {
		return err
	}
	return nil
}

func (i *anIdentifiedDataSerializable) ReadData(input serialization.DataInput) error {
	i.bool = input.ReadBool()
	i.b = input.ReadByte()
	i.c = input.ReadUInt16()
	i.d = input.ReadFloat64()
	i.s = input.ReadInt16()
	i.f = input.ReadFloat32()
	i.i = input.ReadInt32()
	i.l = input.ReadInt64()
	i.str = input.ReadString()

	i.booleans = input.ReadBoolArray()
	i.bytes = input.ReadByteArray()
	i.chars = input.ReadUInt16Array()
	i.doubles = input.ReadFloat64Array()
	i.shorts = input.ReadInt16Array()
	i.floats = input.ReadFloat32Array()
	i.ints = input.ReadInt32Array()
	i.longs = input.ReadInt64Array()
	i.strings = input.ReadUTFArray()

	i.booleansNull = input.ReadBoolArray()
	i.bytesNull = input.ReadByteArray()
	i.charsNull = input.ReadUInt16Array()
	i.doublesNull = input.ReadFloat64Array()
	i.shortsNull = input.ReadInt16Array()
	i.floatsNull = input.ReadFloat32Array()
	i.intsNull = input.ReadInt32Array()
	i.longsNull = input.ReadInt64Array()
	i.stringsNull = input.ReadUTFArray()

	temp := input.ReadObject()

	if temp != nil {
		i.portableObject = temp.(serialization.Portable)
	} else {
		i.portableObject = nil
	}

	temp = input.ReadObject()
	if temp != nil {
		i.identifiedDataSerializableObject = temp.(serialization.IdentifiedDataSerializable)
	} else {
		i.identifiedDataSerializableObject = nil
	}

	return input.Error()
}
