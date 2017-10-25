package api

import . "github.com/hazelcast/go-client/core"

type IdentifiedDataSerializableFactory interface {
	Create(id int32) IdentifiedDataSerializable
}
type IdentifiedDataSerializable interface {
	ReadData(input DataInput) error
	WriteData(output DataOutput)
	FactoryId() int32
	ClassId() int32
}

type Portable interface {
	FactoryId() int32
	ClassId() int32
	WritePortable(writer PortableWriter)
	ReadPortable(reader PortableReader)
}

type VersionedPortable interface {
	Portable
	Version() int32
}

type PortableFactory interface {
	Create(classId int32) Portable
}

type Serializer interface {
	Id() int32
	Read(input DataInput) (interface{}, error)
	Write(output DataOutput, object interface{})
}

type DataOutput interface {
	Position() int32
	SetPosition(pos int32)
	WriteByte(v byte)
	WriteBool(v bool)
	WriteUInt16(v uint16)
	WriteInt16(v int16)
	WriteInt32(v int32)
	WriteInt64(v int64)
	WriteFloat32(v float32)
	WriteFloat64(v float64)
	WriteUTF(v string)
	WriteObject(i interface{})
	WriteData(data IData)
	WriteByteArray(v []byte)
	WriteBoolArray(v []bool)
	WriteUInt16Array(v []uint16)
	WriteInt16Array(v []int16)
	WriteInt32Array(v []int32)
	WriteInt64Array(v []int64)
	WriteFloat32Array(v []float32)
	WriteFloat64Array(v []float64)
	WriteUTFArray(v []string)
	WriteBytes(bytes string)
	WriteZeroBytes(count int)
}

type PositionalDataOutput interface {
	DataOutput
	PWriteByte(position int32, v byte)
	PWriteBool(position int32, v bool)
	PWriteInt16(position int32, v int16)
	PWriteInt32(position int32, v int32)
	PWriteInt64(position int32, v int64)
	PWriteFloat32(position int32, v float32)
	PWriteFloat64(position int32, v float64)
}

type DataInput interface {
	Position() int32
	SetPosition(pos int32)
	ReadByte() (byte, error)
	ReadBool() (bool, error)
	ReadUInt16() (uint16, error)
	ReadInt16() (int16, error)
	ReadInt32() (int32, error)
	ReadInt64() (int64, error)
	ReadFloat32() (float32, error)
	ReadFloat64() (float64, error)
	ReadUTF() (string, error)
	ReadObject() (interface{}, error)
	ReadData() (IData, error)
	ReadByteArray() ([]byte, error)
	ReadBoolArray() ([]bool, error)
	ReadUInt16Array() ([]uint16, error)
	ReadInt16Array() ([]int16, error)
	ReadInt32Array() ([]int32, error)
	ReadInt64Array() ([]int64, error)
	ReadFloat32Array() ([]float32, error)
	ReadFloat64Array() ([]float64, error)
	ReadUTFArray() ([]string, error)
}

type PortableWriter interface {
	WriteByte(fieldName string, value byte)
	WriteBool(fieldName string, value bool)
	WriteUInt16(fieldName string, value uint16)
	WriteInt16(fieldName string, value int16)
	WriteInt32(fieldName string, value int32)
	WriteInt64(fieldName string, value int64)
	WriteFloat32(fieldName string, value float32)
	WriteFloat64(fieldName string, value float64)
	WriteUTF(fieldName string, value string)
	WritePortable(fieldName string, value Portable) error
	WriteNilPortable(fieldName string, factoryId int32, classId int32) error
	WriteByteArray(fieldName string, value []byte)
	WriteBoolArray(fieldName string, value []bool)
	WriteUInt16Array(fieldName string, value []uint16)
	WriteInt16Array(fieldName string, value []int16)
	WriteInt32Array(fieldName string, value []int32)
	WriteInt64Array(fieldName string, value []int64)
	WriteFloat32Array(fieldName string, value []float32)
	WriteFloat64Array(fieldName string, value []float64)
	WriteUTFArray(fieldName string, value []string)
	WritePortableArray(fieldName string, value []Portable) error
	End()
}

type PortableReader interface {
	ReadByte(fieldName string) (byte, error)
	ReadBool(fieldName string) (bool, error)
	ReadUInt16(fieldName string) (uint16, error)
	ReadInt16(fieldName string) (int16, error)
	ReadInt32(fieldName string) (int32, error)
	ReadInt64(fieldName string) (int64, error)
	ReadFloat32(fieldName string) (float32, error)
	ReadFloat64(fieldName string) (float64, error)
	ReadUTF(fieldName string) (string, error)
	ReadPortable(fieldName string) (Portable, error)
	ReadByteArray(fieldName string) ([]byte, error)
	ReadBoolArray(fieldName string) ([]bool, error)
	ReadUInt16Array(fieldName string) ([]uint16, error)
	ReadInt16Array(fieldName string) ([]int16, error)
	ReadInt32Array(fieldName string) ([]int32, error)
	ReadInt64Array(fieldName string) ([]int64, error)
	ReadFloat32Array(fieldName string) ([]float32, error)
	ReadFloat64Array(fieldName string) ([]float64, error)
	ReadUTFArray(fieldName string) ([]string, error)
	ReadPortableArray(fieldName string) ([]Portable, error)
	End()
}
