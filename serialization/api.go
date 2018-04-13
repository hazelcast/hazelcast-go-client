// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

// Package serialization serializes user objects to Data and back to Object.
// Data is the internal representation of binary data in Hazelcast.
package serialization

// IdentifiedDataSerializableFactory is used to create IdentifiedDataSerializable instances during deserialization.
type IdentifiedDataSerializableFactory interface {
	// Creates an IdentifiedDataSerializable instance using given type ID.
	Create(id int32) IdentifiedDataSerializable
}

// IdentifiedDataSerializable is a serialization method as an alternative to standard Gob serialization.
// Each IdentifiedDataSerializable is created by a registered IdentifiedDataSerializableFactory.
type IdentifiedDataSerializable interface {
	// FactoryId returns IdentifiedDataSerializableFactory factory ID for this struct.
	FactoryId() int32

	// ClassId returns type identifier for this struct. It should be unique per IdentifiedDataSerializableFactory.
	ClassId() int32

	// WriteData writes object fields to output stream.
	WriteData(output DataOutput) error

	// ReadData reads fields from the input stream.
	ReadData(input DataInput) error
}

// Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
// created by a registered PortableFactory.
// Portable serialization has the following advantages:
//
// * Supporting multiversion of the same object type.
//
// * Fetching individual fields without having to rely on reflection.
//
// * Querying and indexing support without deserialization and/or reflection.
type Portable interface {
	// FactoryId returns PortableFactory ID for this portable struct.
	FactoryId() int32

	// ClassId returns type identifier for this portable struct. Class ID should be unique per PortableFactory.
	ClassId() int32

	// WritePortable serializes this portable object using PortableWriter.
	WritePortable(writer PortableWriter) error

	// ReadPortable reads portable fields using PortableReader.
	ReadPortable(reader PortableReader) error
}

// VersionedPortable is an extension to Portable
// to support per class version instead of a global serialization version.
type VersionedPortable interface {
	Portable

	// Version returns version for this Portable struct.
	Version() int32
}

// PortableFactory is used to create Portable instances during deserialization.
type PortableFactory interface {
	// Create creates a Portable instance using given class ID and
	// returns portable instance or nil if class ID is not known by this factory.
	Create(classId int32) Portable
}

// Serializer is base interface of serializers.
type Serializer interface {
	// Id returns id of serializer.
	Id() int32

	// Read reads an object from ObjectDataInput.
	Read(input DataInput) (interface{}, error)

	// Write writes an object to ObjectDataOutput.
	Write(output DataOutput, object interface{}) error
}

// IData is the basic unit of serialization. It stores binary form of an object serialized
// by SerializationService's ToData() method.
type IData interface {
	// Buffer returns byte array representation of internal binary format.
	Buffer() []byte

	// GetType returns serialization type of binary form.
	GetType() int32

	// TotalSize returns the total size of Data in bytes.
	TotalSize() int

	// DataSize returns size of internal binary data in bytes.
	DataSize() int

	// GetPartitionHash returns partition hash calculated for serialized object.
	GetPartitionHash() int32
}

// DataOutput provides serialization methods.
type DataOutput interface {
	// Position returns the head position in the byte array.
	Position() int32

	// SetPosition sets the head position in the byte array.
	SetPosition(pos int32)

	// WriteByte writes a byte.
	WriteByte(v byte)

	// WriteBool writes a bool.
	WriteBool(v bool)

	// WriteUInt16 writes an uint16.
	WriteUInt16(v uint16)

	// WriteInt16 writes an int16.
	WriteInt16(v int16)

	// WriteInt32 writes an int32.
	WriteInt32(v int32)

	// WriteInt64 writes an int64.
	WriteInt64(v int64)

	// WriteFloat32 writes a float32.
	WriteFloat32(v float32)

	// WriteFloat64 writes a float64.
	WriteFloat64(v float64)

	// WriteUTF writes a string in UTF-8 format.
	WriteUTF(v string)

	// WriteObject writes an object.
	WriteObject(i interface{}) error

	// WriteData writes an IData.
	WriteData(data IData)

	// WriteByteArray writes a []byte.
	WriteByteArray(v []byte)

	// WriteBoolArray writes a []bool.
	WriteBoolArray(v []bool)

	// WriteUInt16Array writes an []uint16.
	WriteUInt16Array(v []uint16)

	// WriteInt16Array writes an []int16.
	WriteInt16Array(v []int16)

	// WriteInt32Array writes an []int32.
	WriteInt32Array(v []int32)

	// WriteInt64Array writes an []int64.
	WriteInt64Array(v []int64)

	// WriteFloat32Array writes a []float32.
	WriteFloat32Array(v []float32)

	// WriteFloat64Array writes a []float64.
	WriteFloat64Array(v []float64)

	// WriteUTFArray writes a []string in UTF-8 format.
	WriteUTFArray(v []string)

	// WriteBytes writes a string's characters.
	WriteBytes(bytes string)

	// WriteZeroBytes writes zero bytes as given length.
	WriteZeroBytes(count int)
}

// PositionalDataOutput provides some serialization methods for a specific position.
type PositionalDataOutput interface {
	// DataOutput provides serialization methods.
	DataOutput

	// PWriteByte writes a byte to a specific position.
	PWriteByte(position int32, v byte)

	// PWriteBool writes a bool to a specific position.
	PWriteBool(position int32, v bool)

	// PWriteUInt16 writes an uint16 to a specific position.
	PWriteUInt16(position int32, v uint16)

	// PWriteInt16 writes an int16 to a specific position.
	PWriteInt16(position int32, v int16)

	// PWriteInt32 writes an int32 to a specific position.
	PWriteInt32(position int32, v int32)

	// PWriteInt64 writes an int64 to a specific position.
	PWriteInt64(position int32, v int64)

	// PWriteFloat32 writes a float32 to a specific position.
	PWriteFloat32(position int32, v float32)

	// PWriteFloat64 writes a float64 to a specific position.
	PWriteFloat64(position int32, v float64)
}

// DataInput provides deserialization methods.
type DataInput interface {
	// Position returns the head position in the byte array.
	Position() int32

	// SetPosition sets the head position in the byte array.
	SetPosition(pos int32)

	// ReadByte returns byte read and error.
	ReadByte() (byte, error)

	// ReadBool returns bool read and error.
	ReadBool() (bool, error)

	// ReadUInt16 returns uint16 read and error.
	ReadUInt16() (uint16, error)

	// ReadInt16 returns int16 read and error.
	ReadInt16() (int16, error)

	// ReadInt32 returns int32 read and error.
	ReadInt32() (int32, error)

	// ReadInt64 returns int64 read and error.
	ReadInt64() (int64, error)

	// ReadFloat32 returns float32 read and error.
	ReadFloat32() (float32, error)

	// ReadFloat64 returns float64 read and error.
	ReadFloat64() (float64, error)

	// ReadUTF returns string read and error.
	ReadUTF() (string, error)

	// ReadObject returns object read and error.
	ReadObject() (interface{}, error)

	// ReadData returns IData read and error.
	ReadData() (IData, error)

	// ReadByteArray returns []byte read and error.
	ReadByteArray() ([]byte, error)

	// ReadBoolArray returns []bool read and error.
	ReadBoolArray() ([]bool, error)

	// ReadUInt16Array returns []uint16 read and error.
	ReadUInt16Array() ([]uint16, error)

	// ReadInt16Array returns []int16 read and error.
	ReadInt16Array() ([]int16, error)

	// ReadInt32Array returns []int32 read and error.
	ReadInt32Array() ([]int32, error)

	// ReadInt64Array returns []int64 read and error.
	ReadInt64Array() ([]int64, error)

	// ReadFloat32Array returns []float32 read and error.
	ReadFloat32Array() ([]float32, error)

	// ReadFloat64Array returns []float64 read and error.
	ReadFloat64Array() ([]float64, error)

	// ReadUTFArray returns []string read and error.
	ReadUTFArray() ([]string, error)
}

// PortableWriter provides a mean of writing portable fields to a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
type PortableWriter interface {
	// WriteByte writes a byte with fieldName.
	WriteByte(fieldName string, value byte)

	// WriteBool writes a bool with fieldName.
	WriteBool(fieldName string, value bool)

	// WriteUInt16 writes a uint16 with fieldName.
	WriteUInt16(fieldName string, value uint16)

	// WriteInt16 writes a int16 with fieldName.
	WriteInt16(fieldName string, value int16)

	// WriteInt32 writes a int32 with fieldName.
	WriteInt32(fieldName string, value int32)

	// WriteInt64 writes a int64 with fieldName.
	WriteInt64(fieldName string, value int64)

	// WriteFloat32 writes a float32 with fieldName.
	WriteFloat32(fieldName string, value float32)

	// WriteFloat64 writes a float64 with fieldName.
	WriteFloat64(fieldName string, value float64)

	// WriteUTF writes a string in UTF-8 format with fieldName.
	WriteUTF(fieldName string, value string)

	// WritePortable writes a Portable with fieldName.
	WritePortable(fieldName string, value Portable) error

	// WriteNilPortable writes a NilPortable with fieldName, factoryId and classId.
	WriteNilPortable(fieldName string, factoryId int32, classId int32) error

	// WriteByteArray writes a []byte with fieldName.
	WriteByteArray(fieldName string, value []byte)

	// WriteBoolArray writes a []bool with fieldName.
	WriteBoolArray(fieldName string, value []bool)

	// WriteUInt16Array writes a []uint16 with fieldName.
	WriteUInt16Array(fieldName string, value []uint16)

	// WriteInt16Array writes a []int16 with fieldName.
	WriteInt16Array(fieldName string, value []int16)

	// WriteInt32Array writes a []int32 with fieldName.
	WriteInt32Array(fieldName string, value []int32)

	// WriteInt64Array writes a []int64 with fieldName.
	WriteInt64Array(fieldName string, value []int64)

	// WriteFloat32Array writes a []float32 with fieldName.
	WriteFloat32Array(fieldName string, value []float32)

	// WriteFloat64Array writes a []float64 with fieldName.
	WriteFloat64Array(fieldName string, value []float64)

	// WriteUTFArray writes a []string in UTF-8 format with fieldName.
	WriteUTFArray(fieldName string, value []string)

	// WritePortableArray writes a []Portable with fieldName.
	WritePortableArray(fieldName string, value []Portable) error
}

// PortableReader provides a mean of reading portable fields from a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
type PortableReader interface {
	// ReadByte takes fieldName name of the field and returns the byte value read and error.
	ReadByte(fieldName string) (byte, error)

	// ReadBool takes fieldName name of the field and returns the bool value read and error.
	ReadBool(fieldName string) (bool, error)

	// ReadUInt16 takes fieldName name of the field and returns the uint16 value read and error.
	ReadUInt16(fieldName string) (uint16, error)

	// ReadInt16 takes fieldName name of the field and returns the int16 value read and error.
	ReadInt16(fieldName string) (int16, error)

	// ReadInt32 takes fieldName name of the field and returns the int32 value read and error.
	ReadInt32(fieldName string) (int32, error)

	// ReadInt64 takes fieldName name of the field and returns the int64 value read and error.
	ReadInt64(fieldName string) (int64, error)

	// ReadFloat32 takes fieldName name of the field and returns the float32 value read and error.
	ReadFloat32(fieldName string) (float32, error)

	// ReadFloat64 takes fieldName name of the field and returns the float64 value read and error.
	ReadFloat64(fieldName string) (float64, error)

	// ReadUTF takes fieldName name of the field and returns the string value read and error.
	ReadUTF(fieldName string) (string, error)

	// ReadPortable takes fieldName name of the field and returns the Portable value read and error.
	ReadPortable(fieldName string) (Portable, error)

	// ReadByteArray takes fieldName name of the field and returns the []byte value read and error.
	ReadByteArray(fieldName string) ([]byte, error)

	// ReadBoolArray takes fieldName name of the field and returns the []bool value read and error.
	ReadBoolArray(fieldName string) ([]bool, error)

	// ReadUInt16Array takes fieldName name of the field and returns the []uint16 value read and error.
	ReadUInt16Array(fieldName string) ([]uint16, error)

	// ReadInt16Array takes fieldName name of the field and returns the []int16 value read and error.
	ReadInt16Array(fieldName string) ([]int16, error)

	// ReadInt32Array takes fieldName name of the field and returns the []int32 value read and error.
	ReadInt32Array(fieldName string) ([]int32, error)

	// ReadInt64Array takes fieldName name of the field and returns the []int64 value read and error.
	ReadInt64Array(fieldName string) ([]int64, error)

	// ReadFloat32Array takes fieldName name of the field and returns the []float32 value read and error.
	ReadFloat32Array(fieldName string) ([]float32, error)

	// ReadFloat64Array takes fieldName name of the field and returns the []float64 value read and error.
	ReadFloat64Array(fieldName string) ([]float64, error)

	// ReadUTFArray takes fieldName name of the field and returns the []string value read and error.
	ReadUTFArray(fieldName string) ([]string, error)

	// ReadPortableArray takes fieldName name of the field and returns the []Portable value read and error.
	ReadPortableArray(fieldName string) ([]Portable, error)
}

// ClassDefinition defines a class schema for Portable structs.
type ClassDefinition interface {
	// FactoryId returns factory ID of struct.
	FactoryId() int32

	// ClassId returns class ID of struct.
	ClassId() int32

	// Version returns version of struct.
	Version() int32

	// Field returns field definition of field by given name.
	Field(name string) FieldDefinition

	// FieldCount returns the number of fields in struct.
	FieldCount() int
}

// FieldDefinition defines name, type, index of a field.
type FieldDefinition interface {
	// Type returns field type.
	Type() int32

	// Name returns field name.
	Name() string

	// Index returns field index.
	Index() int32

	// ClassId returns class ID of this field's struct.
	ClassId() int32

	// FactoryId returns factory ID of this field's struct.
	FactoryId() int32

	// Version returns version of this field's struct.
	Version() int32
}
