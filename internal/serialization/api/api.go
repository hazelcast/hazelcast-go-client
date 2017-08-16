package api

type IdentifiedDataSerializableFactory interface {
	Create(id int32) IdentifiedDataSerializable
}
type IdentifiedDataSerializable interface {
	ReadData(input DataInput)
	WriteData(output DataOutput)
	GetFactoryId() int32
	GetClassId() int32
}

type Portable interface {
	getFactoryId() int32
	getClassId() int32
	//	WritePortable(writer PortableWriter)
	//	ReadPortable(reader PortableReader)
}

type VersionedPortable interface {
	Portable
	getVersion() int32
}

type PortableFactory interface {
	create(classId PortableFactory) Portable
}

type DataOutput interface {
	WriteByte(v byte)
	WriteBool(v bool)
	WriteInt16(v int16)
	WriteInt32(v int32)
	WriteInt64(v int64)
	WriteFloat32(v float32)
	WriteFloat64(v float64)
	WriteUTF(v string)
	WriteObject(i interface{})
	WriteByteArray(v []byte)
	WriteBoolArray(v []bool)
	WriteInt16Array(v []int16)
	WriteInt32Array(v []int32)
	WriteInt64Array(v []int64)
	WriteFloat32Array(v []float32)
	WriteFloat64Array(v []float64)
	WriteUTFArray(v []string)
}

type DataInput interface {
	ReadByte() (byte, error)
	ReadBool() (bool, error)
	ReadInt16() (int16, error)
	ReadInt32() (int32, error)
	ReadInt64() (int64, error)
	ReadFloat32() (float32, error)
	ReadFloat64() (float64, error)
	ReadUTF() string
	ReadObject() interface{}
	ReadByteArray() []byte
	ReadBoolArray() []bool
	ReadInt16Array() []int16
	ReadInt32Array() []int32
	ReadInt64Array() []int64
	ReadFloat32Array() []float32
	ReadFloat64Array() []float64
	ReadUTFArray() []string
}
