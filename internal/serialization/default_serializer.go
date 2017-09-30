package serialization

import (
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"log"
)

type NilSerializer struct{}

func (*NilSerializer) GetId() int32 {
	return CONSTANT_TYPE_NULL
}

func (*NilSerializer) Read(input DataInput) (interface{}, error) {
	return nil, nil
}

func (*NilSerializer) Write(output DataOutput, i interface{}) {
	// Empty method
}

type IdentifiedDataSerializableSerializer struct {
	factories map[int32]IdentifiedDataSerializableFactory
}

func NewIdentifiedDataSerializableSerializer(factories map[int32]IdentifiedDataSerializableFactory) *IdentifiedDataSerializableSerializer {
	return &IdentifiedDataSerializableSerializer{factories: factories}
}

func (*IdentifiedDataSerializableSerializer) GetId() int32 {
	return CONSTANT_TYPE_DATA_SERIALIZABLE
}

func (idss *IdentifiedDataSerializableSerializer) Read(input DataInput) (interface{}, error) {
	isIdentified, err := input.ReadBool()
	if err != nil {
		return nil, err
	}
	if !isIdentified {
		log.Printf("Native clients does not support Data Serializable. Please use Identified Data Serializable")
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
		err := "There is no IdentifiedDataSerializer factory with id " + string(factoryId) + "."
		log.Printf(err)
	}
	var object = factory.Create(classId)
	err = object.ReadData(input)
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (*IdentifiedDataSerializableSerializer) Write(output DataOutput, i interface{}) {
	r := i.(IdentifiedDataSerializable)
	output.WriteBool(true)
	output.WriteInt32(r.GetFactoryId())
	output.WriteInt32(r.GetClassId())
	r.WriteData(output)
}

type ByteSerializer struct{}

func (*ByteSerializer) GetId() int32 {
	return CONSTANT_TYPE_BYTE
}

func (*ByteSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadByte()
}

func (*ByteSerializer) Write(output DataOutput, i interface{}) {
	output.WriteByte(i.(byte))
}

type BoolSerializer struct{}

func (*BoolSerializer) GetId() int32 {
	return CONSTANT_TYPE_BOOLEAN
}

func (*BoolSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadBool()
}

func (*BoolSerializer) Write(output DataOutput, i interface{}) {
	output.WriteBool(i.(bool))
}

type UInteger16Serializer struct{}

func (*UInteger16Serializer) GetId() int32 {
	return CONSTANT_TYPE_CHAR
}

func (*UInteger16Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUInt16()
}

func (*UInteger16Serializer) Write(output DataOutput, i interface{}) {
	output.WriteUInt16(i.(uint16))
}

type Integer16Serializer struct{}

func (*Integer16Serializer) GetId() int32 {
	return CONSTANT_TYPE_SHORT
}

func (*Integer16Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt16()
}

func (*Integer16Serializer) Write(output DataOutput, i interface{}) {
	output.WriteInt16(i.(int16))
}

type Integer32Serializer struct{}

func (*Integer32Serializer) GetId() int32 {
	return CONSTANT_TYPE_INTEGER
}

func (*Integer32Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt32()
}

func (*Integer32Serializer) Write(output DataOutput, i interface{}) {
	output.WriteInt32(i.(int32))
}

type Integer64Serializer struct{}

func (*Integer64Serializer) GetId() int32 {
	return CONSTANT_TYPE_LONG
}

func (*Integer64Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt64()
}

func (*Integer64Serializer) Write(output DataOutput, i interface{}) {
	output.WriteInt64(i.(int64))
}

type Float32Serializer struct{}

func (*Float32Serializer) GetId() int32 {
	return CONSTANT_TYPE_FLOAT
}

func (*Float32Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat32()
}

func (*Float32Serializer) Write(output DataOutput, i interface{}) {
	output.WriteFloat32(i.(float32))
}

type Float64Serializer struct{}

func (*Float64Serializer) GetId() int32 {
	return CONSTANT_TYPE_DOUBLE
}

func (*Float64Serializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat64()
}

func (*Float64Serializer) Write(output DataOutput, i interface{}) {
	output.WriteFloat64(i.(float64))
}

type StringSerializer struct{}

func (*StringSerializer) GetId() int32 {
	return CONSTANT_TYPE_STRING
}

func (*StringSerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUTF()
}

func (*StringSerializer) Write(output DataOutput, i interface{}) {
	output.WriteUTF(i.(string))
}

type ByteArraySerializer struct{}

func (*ByteArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_BYTE_ARRAY
}

func (*ByteArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadByteArray()
}

func (*ByteArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteByteArray(i.([]byte))
}

type BoolArraySerializer struct{}

func (*BoolArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_BOOLEAN_ARRAY
}

func (*BoolArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadBoolArray()
}

func (*BoolArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteBoolArray(i.([]bool))
}

type UInteger16ArraySerializer struct{}

func (*UInteger16ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_CHAR_ARRAY
}

func (*UInteger16ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUInt16Array()
}

func (*UInteger16ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteUInt16Array(i.([]uint16))
}

type Integer16ArraySerializer struct{}

func (*Integer16ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_SHORT_ARRAY
}

func (*Integer16ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt16Array()
}

func (*Integer16ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteInt16Array(i.([]int16))
}

type Integer32ArraySerializer struct{}

func (*Integer32ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_INTEGER_ARRAY
}

func (*Integer32ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt32Array()
}

func (*Integer32ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteInt32Array(i.([]int32))
}

type Integer64ArraySerializer struct{}

func (*Integer64ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_LONG_ARRAY
}

func (*Integer64ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadInt64Array()
}

func (*Integer64ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteInt64Array(i.([]int64))
}

type Float32ArraySerializer struct{}

func (*Float32ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_FLOAT_ARRAY
}

func (*Float32ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat32Array()
}

func (*Float32ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteFloat32Array(i.([]float32))
}

type Float64ArraySerializer struct{}

func (*Float64ArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_DOUBLE_ARRAY
}

func (*Float64ArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadFloat64Array()
}

func (*Float64ArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteFloat64Array(i.([]float64))
}

type StringArraySerializer struct{}

func (*StringArraySerializer) GetId() int32 {
	return CONSTANT_TYPE_STRING_ARRAY
}

func (*StringArraySerializer) Read(input DataInput) (interface{}, error) {
	return input.ReadUTFArray()
}

func (*StringArraySerializer) Write(output DataOutput, i interface{}) {
	output.WriteUTFArray(i.([]string))
}
