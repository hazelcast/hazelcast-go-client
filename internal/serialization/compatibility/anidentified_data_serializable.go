package compatibility

import (
	"github.com/hazelcast/go-client/internal/serialization/api"
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

	portableObject                   api.Portable
	identifiedDataSerializableObject api.IdentifiedDataSerializable
}

func (*anIdentifiedDataSerializable) ClassId() int32 {
	return DATA_SERIALIZABLE_CLASS_ID
}

func (*anIdentifiedDataSerializable) FactoryId() int32 {
	return IDENTIFIED_DATA_SERIALIZABLE_FACTORY_ID
}

func (i *anIdentifiedDataSerializable) WriteData(output api.DataOutput) {
	output.WriteBool(i.bool)
	output.WriteByte(i.b)
	output.WriteUInt16(i.c)
	output.WriteFloat64(i.d)
	output.WriteInt16(i.s)
	output.WriteFloat32(i.f)
	output.WriteInt32(i.i)
	output.WriteInt64(i.l)
	output.WriteUTF(i.str)

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

	output.WriteObject(i.portableObject)
	output.WriteObject(i.identifiedDataSerializableObject)

	//output.WriteData(i.data)

}

func (i *anIdentifiedDataSerializable) ReadData(input api.DataInput) error {
	i.bool, _ = input.ReadBool()
	i.b, _ = input.ReadByte()
	i.c, _ = input.ReadUInt16()
	i.d, _ = input.ReadFloat64()
	i.s, _ = input.ReadInt16()
	i.f, _ = input.ReadFloat32()
	i.i, _ = input.ReadInt32()
	i.l, _ = input.ReadInt64()
	i.str, _ = input.ReadUTF()

	i.booleans, _ = input.ReadBoolArray()
	i.bytes, _ = input.ReadByteArray()
	i.chars, _ = input.ReadUInt16Array()
	i.doubles, _ = input.ReadFloat64Array()
	i.shorts, _ = input.ReadInt16Array()
	i.floats, _ = input.ReadFloat32Array()
	i.ints, _ = input.ReadInt32Array()
	i.longs, _ = input.ReadInt64Array()
	i.strings, _ = input.ReadUTFArray()

	i.booleansNull, _ = input.ReadBoolArray()
	i.bytesNull, _ = input.ReadByteArray()
	i.charsNull, _ = input.ReadUInt16Array()
	i.doublesNull, _ = input.ReadFloat64Array()
	i.shortsNull, _ = input.ReadInt16Array()
	i.floatsNull, _ = input.ReadFloat32Array()
	i.intsNull, _ = input.ReadInt32Array()
	i.longsNull, _ = input.ReadInt64Array()
	i.stringsNull, _ = input.ReadUTFArray()

	temp, _ := input.ReadObject()

	if temp != nil {
		i.portableObject = temp.(api.Portable)
	} else {
		i.portableObject = nil
	}

	temp, _ = input.ReadObject()
	if temp != nil {
		i.identifiedDataSerializableObject = temp.(api.IdentifiedDataSerializable)
	} else {
		i.identifiedDataSerializableObject = nil
	}

	return nil
}
