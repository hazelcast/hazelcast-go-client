package api

type IdentifiedDataSerializableFactory interface {
	Create(id int32) IdentifiedDataSerializable
}
type IdentifiedDataSerializable interface {
	ReadData(input DataInput) interface{}
	WriteData(output DataOutput)
	GetFactoryId() int32
	GetClassId() int32
}

type DataOutput interface {
	WriteInt16(v int16)
	WriteInt32(v int32)
	WriteInt64(v int64)
	WriteFloat32(v float32)
	WriteFloat64(v float64)
	WriteBool(v bool)
	WriteUTF(v string)
	WriteObject(i interface{})
	WriteByte(v byte)
	WriteInt16Array(v []int16)
	WriteInt32Array(v []int32)
	WriteInt64Array(v []int64)
	WriteFloat32Array(v []float32)
	WriteFloat64Array(v []float64)

}

type DataInput interface {
	ReadInt16() (int16, error)
	ReadInt32() (int32, error)
	ReadInt32WithPosition(position int) (int32, error)
	ReadInt64() (int64, error)
	ReadFloat32() (float32, error)
	ReadFloat64() (float64, error)
	ReadFloat64WithPosition(position int) (float64, error)
	ReadBool() (bool, error)
	ReadBoolWithPosition(position int) (bool, error)
	ReadUTF() string
	ReadObject() interface{}
	ReadByte() (byte, error)
	ReadInt16Array() []int16
	ReadInt32Array() []int32
	ReadInt64Array() []int64
	ReadFloat32Array() []float32
	ReadFloat64Array() []float64
}
