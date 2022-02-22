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
	"github.com/hazelcast/hazelcast-go-client/types"
)

// IdentifiedDataSerializableFactory is used to create IdentifiedDataSerializable instances during deserialization.
type IdentifiedDataSerializableFactory interface {
	// Create ceates an IdentifiedDataSerializable instance using given type ID.
	Create(id int32) IdentifiedDataSerializable
	// FactoryID returns the factory ID.
	FactoryID() int32
}

// IdentifiedDataSerializable is a serialization method as an alternative to standard Gob serialization.
// Each IdentifiedDataSerializable is created by a registered IdentifiedDataSerializableFactory.
type IdentifiedDataSerializable interface {
	// FactoryID returns IdentifiedDataSerializableFactory factory ID for this struct.
	FactoryID() int32

	// ClassID returns type identifier for this struct. It should be unique per IdentifiedDataSerializableFactory.
	ClassID() int32

	// WriteData writes object fields to output stream.
	WriteData(output DataOutput)

	// ReadData reads fields from the input stream.
	ReadData(input DataInput)
}

// Portable provides an alternative serialization method. Instead of relying on reflection, each Portable is
// created by a registered PortableFactory.
// Portable serialization has the following advantages:
// * Supporting multiversion of the same object type.
// * Fetching individual fields without having to rely on reflection.
// * Querying and indexing support without deserialization and/or reflection.
type Portable interface {
	// FactoryID returns PortableFactory ID for this portable struct.
	FactoryID() int32

	// ClassID returns type identifier for this portable struct. Class ID should be unique per PortableFactory.
	ClassID() int32

	// WritePortable serializes this portable object using PortableWriter.
	WritePortable(writer PortableWriter)

	// ReadPortable reads portable fields using PortableReader.
	ReadPortable(reader PortableReader)
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
	Create(classID int32) Portable
	// FactoryID returns the factory ID.
	FactoryID() int32
}

// Serializer is base interface of serializers.
type Serializer interface {
	// ID returns id of serializer.
	ID() int32

	// Read reads an object from ObjectDataInput.
	Read(input DataInput) interface{}

	// Write writes an object to ObjectDataOutput.
	Write(output DataOutput, object interface{})
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

	// WriteString writes a string in UTF-8 format.
	WriteString(v string)

	// WriteObject writes an object.
	WriteObject(i interface{})

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

	// WriteStringArray writes a []string in UTF-8 format.
	WriteStringArray(v []string)

	// WriteStringBytes writes a string's characters.
	WriteStringBytes(bytes string)

	// WriteZeroBytes writes zero bytes as given length.
	WriteZeroBytes(count int)
}

// DataInput provides deserialization methods.
// If any of the methods results in an error, all following methods will return the zero value
// for that type immediately.
// Example usage:
//  field1 = input.ReadString()
//  field2 = input.ReadString()
type DataInput interface {
	// Position returns the head position in the byte array.
	Position() int32
	// SetPosition sets the head position in the byte array.
	SetPosition(pos int32)
	// ReadByte returns byte read .
	// It returns zero if an error is set previously.
	ReadByte() byte
	// ReadBool returns bool read.
	// It returns false if an error is set previously.
	ReadBool() bool
	// ReadUInt16 returns uint16 read.
	// It returns zero if an error is set previously.
	ReadUInt16() uint16
	// ReadInt16 returns int16 read.
	// It returns zero if an error is set previously.
	ReadInt16() int16
	// ReadInt32 returns int32 read.
	// It returns zero if an error is set previously.
	ReadInt32() int32
	// ReadInt64 returns int64 read.
	// It returns zero if an error is set previously.
	ReadInt64() int64
	// ReadFloat32 returns float32 read.
	// It returns zero if an error is set previously.
	ReadFloat32() float32
	// ReadFloat64 returns float64 read.
	// It returns zero if an error is set previously.
	ReadFloat64() float64
	// ReadString returns string read.
	// It returns empty string if an error is set previously.
	ReadString() string
	// ReadObject returns object read.
	// It returns nil if an error is set previously.
	ReadObject() interface{}
	// ReadByteArray returns []byte read.
	// It returns nil if an error is set previously.
	ReadByteArray() []byte
	// ReadBoolArray returns []bool read.
	// It returns nil if an error is set previously.
	ReadBoolArray() []bool
	// ReadUInt16Array returns []uint16 read.
	// It returns nil if an error is set previously.
	ReadUInt16Array() []uint16
	// ReadInt16Array returns []int16 read.
	// It returns nil if an error is set previously.
	ReadInt16Array() []int16
	// ReadInt32Array returns []int32 read.
	// It returns nil if an error is set previously.
	ReadInt32Array() []int32
	// ReadInt64Array returns []int64 read.
	// It returns nil if an error is set previously.
	ReadInt64Array() []int64
	// ReadFloat32Array returns []float32 read.
	// It returns nil if an error is set previously.
	ReadFloat32Array() []float32
	// ReadFloat64Array returns []float64 read.
	// It returns nil if an error is set previously.
	ReadFloat64Array() []float64
	// ReadStringArray returns []string read.
	// It returns nil if an error is set previously.
	ReadStringArray() []string
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
	// WriteString writes a string in UTF-8 format with fieldName.
	WriteString(fieldName string, value string)
	// WritePortable writes a Portable with fieldName.
	WritePortable(fieldName string, value Portable)
	// WriteNilPortable writes a NilPortable with fieldName, factoryID and classID.
	WriteNilPortable(fieldName string, factoryID int32, classID int32)
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
	// WriteStringArray writes a []string in UTF-8 format with fieldName.
	WriteStringArray(fieldName string, value []string)
	// WritePortableArray writes a []Portable with fieldName.
	WritePortableArray(fieldName string, value []Portable)
	// GetRawDataOutput returns raw DataOutput to write unnamed fields like IdentifiedDataSerializable does.
	// All unnamed fields must be written after portable fields.
	// Attempts to write named fields after GetRawDataOutput is called will panic.
	GetRawDataOutput() DataOutput
	// WriteDate writes a date.
	WriteDate(fieldName string, t *types.LocalDate)
	// WriteTime writes a time.
	WriteTime(fieldName string, t *types.LocalTime)
	// WriteTimestamp writes the date and time without the timezone offset.
	WriteTimestamp(fieldName string, t *types.LocalDateTime)
	// WriteTimestampWithTimezone writes the date and time with the timezone offset.
	WriteTimestampWithTimezone(fieldName string, t *types.OffsetDateTime)
	// WriteDateArray writes a date array.
	WriteDateArray(fieldName string, t []types.LocalDate)
	// WriteTimeArray writes a time array.
	WriteTimeArray(fieldName string, t []types.LocalTime)
	// WriteTimestampArray writes dates and times.
	WriteTimestampArray(fieldName string, t []types.LocalDateTime)
	// WriteTimestampWithTimezoneArray writes date and time with the timezone offsets.
	WriteTimestampWithTimezoneArray(fieldName string, t []types.OffsetDateTime)
	// WriteDecimal writes the given decimal value.
	// The decimal value may be nil.
	WriteDecimal(fieldName string, d *types.Decimal)
	// WriteDecimalArray writes the given decimal array.
	WriteDecimalArray(fieldName string, ds []types.Decimal)
}

// PortableReader provides a mean of reading portable fields from a binary in form of go primitives
// arrays of go primitives, nested portable fields and array of portable fields.
// Example usage:
// 	s.id = reader.ReadInt16("id")
//  s.age = reader.ReadInt32("age")
//  return reader.Error()
type PortableReader interface {
	// ReadByte takes fieldName Name of the field and returns the byte value read.
	// It returns zero if an error is set previously.
	ReadByte(fieldName string) byte
	// ReadBool takes fieldName Name of the field and returns the bool value read.
	// It returns false if an error is set previously.
	ReadBool(fieldName string) bool
	// ReadUInt16 takes fieldName Name of the field and returns the uint16 value read.
	// It returns zero if an error is set previously.
	ReadUInt16(fieldName string) uint16
	// ReadInt16 takes fieldName Name of the field and returns the int16 value read.
	// It returns zero if an error is set previously.
	ReadInt16(fieldName string) int16
	// ReadInt32 takes fieldName Name of the field and returns the int32 value read.
	// It returns zero if an error is set previously.
	ReadInt32(fieldName string) int32
	// ReadInt64 takes fieldName Name of the field and returns the int64 value read.
	// It returns zero if an error is set previously.
	ReadInt64(fieldName string) int64
	// ReadFloat32 takes fieldName Name of the field and returns the float32 value read.
	// It returns zero if an error is set previously.
	ReadFloat32(fieldName string) float32
	// ReadFloat64 takes fieldName Name of the field and returns the float64 value read.
	// It returns zero if an error is set previously.
	ReadFloat64(fieldName string) float64
	// ReadString takes fieldName Name of the field and returns the string value read.
	// It returns empty string if an error is set previously.
	ReadString(fieldName string) string
	// ReadPortable takes fieldName Name of the field and returns the Portable value read.
	// It returns nil if an error is set previously.
	ReadPortable(fieldName string) Portable
	// ReadByteArray takes fieldName Name of the field and returns the []byte value read.
	// It returns nil if an error is set previously.
	ReadByteArray(fieldName string) []byte
	// ReadBoolArray takes fieldName Name of the field and returns the []bool value read.
	// It returns nil if an error is set previously.
	ReadBoolArray(fieldName string) []bool
	// ReadUInt16Array takes fieldName Name of the field and returns the []uint16 value read.
	// It returns nil if an error is set previously.
	ReadUInt16Array(fieldName string) []uint16
	// ReadInt16Array takes fieldName Name of the field and returns the []int16 value read.
	// It returns nil if an error is set previously.
	ReadInt16Array(fieldName string) []int16
	// ReadInt32Array takes fieldName Name of the field and returns the []int32 value read.
	// It returns nil if an error is set previously.
	ReadInt32Array(fieldName string) []int32
	// ReadInt64Array takes fieldName Name of the field and returns the []int64 value read.
	// It returns nil if an error is set previously.
	ReadInt64Array(fieldName string) []int64
	// ReadFloat32Array takes fieldName Name of the field and returns the []float32 value read.
	// It returns nil if an error is set previously.
	ReadFloat32Array(fieldName string) []float32
	// ReadFloat64Array takes fieldName Name of the field and returns the []float64 value read.
	// It returns nil if an error is set previously.
	ReadFloat64Array(fieldName string) []float64
	// ReadStringArray takes fieldName Name of the field and returns the []string value read.
	// It returns nil if an error is set previously.
	ReadStringArray(fieldName string) []string
	// ReadPortableArray takes fieldName Name of the field and returns the []Portable value read.
	// It returns nil if an error is set previously.
	ReadPortableArray(fieldName string) []Portable
	// GetRawDataInput returns raw DataInput to read unnamed fields like
	// IdentifiedDataSerializable does. All unnamed fields must be read after
	// portable fields. Attempts to read named fields after GetRawDataInput is
	// called will panic.
	GetRawDataInput() DataInput
	// ReadDate reads the date.
	// It may return nil.
	ReadDate(fieldName string) *types.LocalDate
	// ReadTime reads the time.
	// It may return nil.
	ReadTime(fieldName string) *types.LocalTime
	// ReadTimestamp reads the time stamp.
	// It may return nil.
	ReadTimestamp(fieldName string) *types.LocalDateTime
	// ReadTimestampWithTimezone reads the time stamp with time zone.
	// It may return nil.
	ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime
	// ReadDateArray reads the date arrau.
	ReadDateArray(fieldName string) []types.LocalDate
	// ReadTimeArray reads the time array.
	ReadTimeArray(fieldName string) []types.LocalTime
	// ReadTimestampArray reads the time stamp array.
	ReadTimestampArray(fieldName string) []types.LocalDateTime
	// ReadTimestampWithTimezoneArray reads the time stamp with time zone array.
	ReadTimestampWithTimezoneArray(fieldName string) []types.OffsetDateTime
	// ReadDecimal reads a decimal.
	// It may return nil.
	ReadDecimal(fieldName string) (d *types.Decimal)
	// ReadDecimalArray a decimal array.
	ReadDecimalArray(fieldName string) (ds []types.Decimal)
}
