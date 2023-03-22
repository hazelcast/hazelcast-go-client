/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/types"
)

// IdentifiedDataSerializableFactory is used to create IdentifiedDataSerializable instances during deserialization.
type IdentifiedDataSerializableFactory interface {
	// Create creates an IdentifiedDataSerializable instance using given type ID.
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

/*
Portable provides an alternative serialization method.
Instead of relying on reflection, each Portable is created by a registered PortableFactory.
Portable serialization has the following advantages:
  - Supporting multiversion of the same object type.
  - Fetching individual fields without having to rely on reflection.
  - Querying and indexing support without deserialization and/or reflection.
*/
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
//
//	field1 = input.ReadString()
//	field2 = input.ReadString()
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
	// WriteTimestampArray writes a timestamp array.
	WriteTimestampArray(fieldName string, t []types.LocalDateTime)
	// WriteTimestampWithTimezoneArray writes a timestamp with timezone array.
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
//
//		s.id = reader.ReadInt16("id")
//	 s.age = reader.ReadInt32("age")
//	 return reader.Error()
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
	// ReadDateArray reads the date array.
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
	// ReadDecimalArray reads a decimal array.
	ReadDecimalArray(fieldName string) (ds []types.Decimal)
}

type FieldKind int32

const (
	FieldKindNotAvailable                 FieldKind = 0
	FieldKindBoolean                      FieldKind = 1
	FieldKindArrayOfBoolean               FieldKind = 2
	FieldKindInt8                         FieldKind = 3
	FieldKindArrayOfInt8                  FieldKind = 4
	FieldKindChar                         FieldKind = 5
	FieldKindArrayOfChar                  FieldKind = 6
	FieldKindInt16                        FieldKind = 7
	FieldKindArrayOfInt16                 FieldKind = 8
	FieldKindInt32                        FieldKind = 9
	FieldKindArrayOfInt32                 FieldKind = 10
	FieldKindInt64                        FieldKind = 11
	FieldKindArrayOfInt64                 FieldKind = 12
	FieldKindFloat32                      FieldKind = 13
	FieldKindArrayOfFloat32               FieldKind = 14
	FieldKindFloat64                      FieldKind = 15
	FieldKindArrayOfFloat64               FieldKind = 16
	FieldKindString                       FieldKind = 17
	FieldKindArrayOfString                FieldKind = 18
	FieldKindDecimal                      FieldKind = 19
	FieldKindArrayOfDecimal               FieldKind = 20
	FieldKindTime                         FieldKind = 21
	FieldKindArrayOfTime                  FieldKind = 22
	FieldKindDate                         FieldKind = 23
	FieldKindArrayOfDate                  FieldKind = 24
	FieldKindTimestamp                    FieldKind = 25
	FieldKindArrayOfTimestamp             FieldKind = 26
	FieldKindTimestampWithTimezone        FieldKind = 27
	FieldKindArrayOfTimestampWithTimezone FieldKind = 28
	FieldKindCompact                      FieldKind = 29
	FieldKindArrayOfCompact               FieldKind = 30
	FieldKindPortable                     FieldKind = 31
	FieldKindArrayOfPortable              FieldKind = 32
	FieldKindNullableBoolean              FieldKind = 33
	FieldKindArrayOfNullableBoolean       FieldKind = 34
	FieldKindNullableInt8                 FieldKind = 35
	FieldKindArrayOfNullableInt8          FieldKind = 36
	FieldKindNullableInt16                FieldKind = 37
	FieldKindArrayOfNullableInt16         FieldKind = 38
	FieldKindNullableInt32                FieldKind = 39
	FieldKindArrayOfNullableInt32         FieldKind = 40
	FieldKindNullableInt64                FieldKind = 41
	FieldKindArrayOfNullableInt64         FieldKind = 42
	FieldKindNullableFloat32              FieldKind = 43
	FieldKindArrayOfNullableFloat32       FieldKind = 44
	FieldKindNullableFloat64              FieldKind = 45
	FieldKindArrayOfNullableFloat64       FieldKind = 46
)

// CompactSerializer must be implemented to serialize a value with compact serialization.
type CompactSerializer interface {
	// Type returns the target type for this serializer.
	Type() reflect.Type
	/*
		TypeName returns a string which uniquely identifies this serializers.
		Choosing a type name will associate that name with the schema and will make the polyglot use cases, where there are multiple clients from different languages, possible.
		Serializers in different languages can work on the same data, provided that their read and write methods are compatible, and they have the same type name.
		If you evolve your class in the later versions of your application, by adding or removing fields, you should continue using the same type name for that class.
	*/
	TypeName() string
	// Read reads the fields from reader and creates a value of the type returned from Type()
	Read(reader CompactReader) interface{}
	// Write writes the fields of value using the writer.
	Write(writer CompactWriter, value interface{})
}

// CompactReader is an interface implemented by types passed to CompactSerializer.Read method.
type CompactReader interface {
	// ReadBoolean reads and returns a boolean.
	ReadBoolean(fieldName string) bool
	// ReadInt8 reads and returns an int8.
	ReadInt8(fieldName string) int8
	// ReadInt16 reads and returns and int16
	ReadInt16(fieldName string) int16
	// ReadInt32 reads and returns an int32
	ReadInt32(fieldName string) int32
	// ReadInt64 reads and returns an int64
	ReadInt64(fieldName string) int64
	// ReadFloat32 reads and returns a float32
	ReadFloat32(fieldName string) float32
	// ReadFloat64 reads and returns a float64
	ReadFloat64(fieldName string) float64
	// ReadString reads and returns a string which may be nil.
	ReadString(fieldName string) *string
	// ReadDecimal reads and returns a *types.Decimal value which may be nil.
	ReadDecimal(fieldName string) *types.Decimal
	// ReadTime reads and returns a *types.LocalTime value which may be nil.
	ReadTime(fieldName string) *types.LocalTime
	// ReadDate reads and returns a *types.LocalDate value which may be nil.
	ReadDate(fieldName string) *types.LocalDate
	// ReadTimestamp reads and returns a *types.LocalDateTime value which may be nil.
	ReadTimestamp(fieldName string) *types.LocalDateTime
	// ReadTimestampWithTimezone reads and returns a *types.OffsetDateTime value which may be nil.
	ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime
	// ReadCompact reads and returns a compact serialized value which may be nil.
	ReadCompact(fieldName string) interface{}
	// ReadArrayOfBoolean reads and returns a slice of bools.
	ReadArrayOfBoolean(fieldName string) []bool
	// ReadArrayOfInt8 reads and returns a slice of int8s.
	ReadArrayOfInt8(fieldName string) []int8
	// ReadArrayOfInt16 reads and returns a slice of int16s.
	ReadArrayOfInt16(fieldName string) []int16
	// ReadArrayOfInt32 reads and returns a slice of int32s.
	ReadArrayOfInt32(fieldName string) []int32
	// ReadArrayOfInt64 reads and returns a slice of int64s.
	ReadArrayOfInt64(fieldName string) []int64
	// ReadArrayOfFloat32 reads and returns a slice of float32s.
	ReadArrayOfFloat32(fieldName string) []float32
	// ReadArrayOfFloat64 reads and returns a slice of float64s.
	ReadArrayOfFloat64(fieldName string) []float64
	// ReadArrayOfString reads and returns a slice of string pointers.
	// The returned pointers may be nil.
	ReadArrayOfString(fieldName string) []*string
	// ReadArrayOfDecimal reads and returns a slice of type.Decimal pointers.
	// The returned pointers may be nil.
	ReadArrayOfDecimal(fieldName string) []*types.Decimal
	// ReadArrayOfTime reads and returns a slice of type.LocalTime pointers.
	// The returned pointers may be nil.
	ReadArrayOfTime(fieldName string) []*types.LocalTime
	// ReadArrayOfDate reads and returns a slice of type.LocalDate pointers.
	// The returned pointers may be nil.
	ReadArrayOfDate(fieldName string) []*types.LocalDate
	// ReadArrayOfTimestamp reads and returns a slice of type.LocalDateTime pointers.
	// The returned pointers may be nil.
	ReadArrayOfTimestamp(fieldName string) []*types.LocalDateTime
	// ReadArrayOfTimestampWithTimezone reads and returns a slice of type.OffsetDateTime pointers.
	// The returned pointers may be nil.
	ReadArrayOfTimestampWithTimezone(fieldName string) []*types.OffsetDateTime
	// ReadArrayOfCompact reads and returns a slice of values.
	ReadArrayOfCompact(fieldName string) []interface{}
	// ReadNullableBoolean reads and returns a bool pointer.
	// The returned pointer may be nil.
	ReadNullableBoolean(fieldName string) *bool
	// ReadNullableInt8 reads and returns an int8 pointer.
	// The returned pointer may be nil.
	ReadNullableInt8(fieldName string) *int8
	// ReadNullableInt16 reads and returns an int16 pointer.
	// The returned pointer may be nil.
	ReadNullableInt16(fieldName string) *int16
	// ReadNullableInt32 reads and returns an int32 pointer.
	// The returned pointer may be nil.
	ReadNullableInt32(fieldName string) *int32
	// ReadNullableInt64 reads and returns an int64 pointer.
	// The returned pointer may be nil.
	ReadNullableInt64(fieldName string) *int64
	// ReadNullableFloat32 reads and returns a float32 pointer.
	// The returned pointer may be nil.
	ReadNullableFloat32(fieldName string) *float32
	// ReadNullableFloat64 reads and returns a float64 pointer.
	// The returned pointer may be nil.
	ReadNullableFloat64(fieldName string) *float64
	// ReadArrayOfNullableBoolean reads and returns a slice of bool pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableBoolean(fieldName string) []*bool
	// ReadArrayOfNullableInt8 reads and returns a slice of int8 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableInt8(fieldName string) []*int8
	// ReadArrayOfNullableInt16 reads and returns a slice of int16 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableInt16(fieldName string) []*int16
	// ReadArrayOfNullableInt32 reads and returns a slice of int32 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableInt32(fieldName string) []*int32
	// ReadArrayOfNullableInt64 reads and returns a slice of int64 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableInt64(fieldName string) []*int64
	// ReadArrayOfNullableFloat32 reads and returns a slice of float32 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableFloat32(fieldName string) []*float32
	// ReadArrayOfNullableFloat64 reads and returns a slice of float64 pointers.
	// The returned pointers may be nil.
	ReadArrayOfNullableFloat64(fieldName string) []*float64
	// GetFieldKind returns the kind of the given field.
	// If the field does not exist, it returns FieldKindNotAvailable
	GetFieldKind(fieldName string) FieldKind
}

// CompactWriter is an interface implemented by types passed to CompactSerializer.Write method.
type CompactWriter interface {
	// WriteBoolean writes a bool value.
	WriteBoolean(fieldName string, value bool)
	// WriteInt8 writes an int8 value.
	WriteInt8(fieldName string, value int8)
	// WriteInt16 writes an int16 value.
	WriteInt16(fieldName string, value int16)
	// WriteInt32 writes an int32 value.
	WriteInt32(fieldName string, value int32)
	// WriteInt64 writes an int64 value.
	WriteInt64(fieldName string, value int64)
	// WriteFloat32 writes a float32 value.
	WriteFloat32(fieldName string, value float32)
	// WriteFloat64 writes a float64 value.
	WriteFloat64(fieldName string, value float64)
	// WriteString writes a string pointer value.
	// The pointer may be nil.
	WriteString(fieldName string, value *string)
	// WriteDecimal writes a types.Decimal pointer value.
	// The pointer may be nil.
	WriteDecimal(fieldName string, value *types.Decimal)
	// WriteTime writes a types.LocalTime pointer value.
	// The pointer may be nil.
	WriteTime(fieldName string, value *types.LocalTime)
	// WriteDate writes a types.LocalDate pointer value.
	// The pointer may be nil.
	WriteDate(fieldName string, value *types.LocalDate)
	// WriteTimestamp writes a types.LocalDateTime pointer value.
	// The pointer may be nil.
	WriteTimestamp(fieldName string, value *types.LocalDateTime)
	// WriteTimestampWithTimezone writes a types.OffsetDateTime pointer value.
	// The pointer may be nil.
	WriteTimestampWithTimezone(fieldName string, value *types.OffsetDateTime)
	// WriteCompact writes a value which can be serialized by a CompactSerializer.
	WriteCompact(fieldName string, value interface{})
	// WriteArrayOfBoolean writes a slice of bools.
	WriteArrayOfBoolean(fieldName string, value []bool)
	// WriteArrayOfInt8 writes a slice of int8s.
	WriteArrayOfInt8(fieldName string, value []int8)
	// WriteArrayOfInt16 writes a slice of int16s.
	WriteArrayOfInt16(fieldName string, value []int16)
	// WriteArrayOfInt32 writes a slice of int32s.
	WriteArrayOfInt32(fieldName string, value []int32)
	// WriteArrayOfInt64 writes a slice of int64s.
	WriteArrayOfInt64(fieldName string, value []int64)
	// WriteArrayOfFloat32 writes a slice of float32s.
	WriteArrayOfFloat32(fieldName string, value []float32)
	// WriteArrayOfFloat64 writes a slice of float64s.
	WriteArrayOfFloat64(fieldName string, value []float64)
	// WriteArrayOfString writes a slice of string pointers.
	// The pointers may be nil.
	WriteArrayOfString(fieldName string, value []*string)
	// WriteArrayOfDecimal writes a slice of types.Decimal pointers.
	// The pointers may be nil.
	WriteArrayOfDecimal(fieldName string, value []*types.Decimal)
	// WriteArrayOfTime writes a slice of types.LocalTime pointers.
	// The pointers may be nil.
	WriteArrayOfTime(fieldName string, value []*types.LocalTime)
	// WriteArrayOfDate writes a slice of types.LocalDate pointers.
	// The pointers may be nil.
	WriteArrayOfDate(fieldName string, value []*types.LocalDate)
	// WriteArrayOfTimestamp writes a slice of types.LocalDateTime pointers.
	// The pointers may be nil.
	WriteArrayOfTimestamp(fieldName string, value []*types.LocalDateTime)
	// WriteArrayOfTimestampWithTimezone writes a slice of types.OffsetDateTime pointers.
	// The pointers may be nil.
	WriteArrayOfTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime)
	// WriteArrayOfCompact writes a slice of values which can be serialized using a CompactSerializer.
	WriteArrayOfCompact(fieldName string, value []interface{})
	// WriteNullableBoolean writes a bool pointer.
	// The pointer may be nil.
	WriteNullableBoolean(fieldName string, value *bool)
	// WriteNullableInt8 writes an int8 pointer.
	// The pointer may be nil.
	WriteNullableInt8(fieldName string, value *int8)
	// WriteNullableInt16 writes an int16 pointer.
	// The pointer may be nil.
	WriteNullableInt16(fieldName string, value *int16)
	// WriteNullableInt32 writes an int32 pointer.
	// The pointer may be nil.
	WriteNullableInt32(fieldName string, value *int32)
	// WriteNullableInt64 writes an int64 pointer.
	// The pointer may be nil.
	WriteNullableInt64(fieldName string, value *int64)
	// WriteNullableFloat32 writes a float32 pointer.
	// The pointer may be nil.
	WriteNullableFloat32(fieldName string, value *float32)
	// WriteNullableFloat64 writes a float64 pointer.
	// The pointer may be nil.
	WriteNullableFloat64(fieldName string, value *float64)
	// WriteArrayOfNullableBoolean writes a slice of bool pointers.
	// The pointers may be nil.
	WriteArrayOfNullableBoolean(fieldName string, value []*bool)
	// WriteArrayOfNullableInt8 writes a slice of int8 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableInt8(fieldName string, value []*int8)
	// WriteArrayOfNullableInt16 writes a slice of int16 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableInt16(fieldName string, value []*int16)
	// WriteArrayOfNullableInt32 writes a slice of int32 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableInt32(fieldName string, value []*int32)
	// WriteArrayOfNullableInt64 writes a slice of int64 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableInt64(fieldName string, value []*int64)
	// WriteArrayOfNullableFloat32 writes a slice of float32 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableFloat32(fieldName string, value []*float32)
	// WriteArrayOfNullableFloat64 writes a slice of float64 pointers.
	// The pointers may be nil.
	WriteArrayOfNullableFloat64(fieldName string, value []*float64)
}
