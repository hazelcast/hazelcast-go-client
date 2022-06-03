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
	"fmt"
	"time"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	nullOffset             = -1
	byteOffsetReaderRange  = 127 - (-128)
	shortOffsetReaderRange = 32767 - (-32768)
)

type Reader func(*ObjectDataInput) interface{}
type SliceReader func(*ObjectDataInput, int32, bool)
type SliceConstructor func(int)

var byteOffsetReader = ByteOffsetReader{}
var shortOffsetReader = ShortOffsetReader{}
var intOffsetReader = IntOffsetReader{}

type OffsetReader interface {
	getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32
}

type ByteOffsetReader struct{}

func (ByteOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos int32, index int32) int32 {
	offset := inp.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return nullOffset
	}
	return int32(offset)
}

type ShortOffsetReader struct{}

func (ShortOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos int32, index int32) int32 {
	offset := inp.ReadInt16AtPosition(variableOffsetsPos + (index * Int16SizeInBytes))
	return int32(offset)
}

type IntOffsetReader struct{}

func (IntOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos int32, index int32) int32 {
	return inp.ReadInt32AtPosition(variableOffsetsPos + (index * Int32SizeInBytes))
}

type DefaultCompactReader struct {
	offsetReader OffsetReader
	in           *ObjectDataInput
	serializer   CompactStreamSerializer
	schema       Schema
	startPos     int32
	offsetsPos   int32
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	var varOffsetsPos, startPos, finalPos int32
	var offsetReader OffsetReader
	if schema.numberOfVarSizeFields == 0 {
		offsetReader = intOffsetReader
		varOffsetsPos = 0
		startPos = input.position
		finalPos = startPos + schema.fixedSizeFieldsLength
	} else {
		dataLength := input.readInt32()
		startPos = input.position
		varOffsetsPos = startPos + dataLength
		if dataLength < byteOffsetReaderRange {
			offsetReader = byteOffsetReader
			finalPos = varOffsetsPos + schema.numberOfVarSizeFields
		} else if dataLength < shortOffsetReaderRange {
			offsetReader = shortOffsetReader
			finalPos = varOffsetsPos + schema.numberOfVarSizeFields*Int16SizeInBytes
		} else {
			offsetReader = intOffsetReader
			finalPos = varOffsetsPos + schema.numberOfVarSizeFields*Int32SizeInBytes
		}
	}
	input.SetPosition(finalPos)
	return DefaultCompactReader{
		schema:       schema,
		in:           input,
		serializer:   serializer,
		startPos:     startPos,
		offsetReader: offsetReader,
		offsetsPos:   varOffsetsPos,
	}
}

func (r DefaultCompactReader) ReadBoolean(fieldName string) bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindBoolean:
		return r.readBoolean(fd)
	case pubserialization.FieldKindNullableBoolean:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadBool()
		}, "Boolean").(bool)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt8(fieldName string) int8 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt8:
		return r.in.ReadSignedByteAtPosition(r.getFixedSizePosition(fd))
	case pubserialization.FieldKindNullableInt8:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadSignedByte()
		}, "Int8").(int8)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt16(fieldName string) int16 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt16:
		return r.in.ReadInt16AtPosition(r.getFixedSizePosition(fd))
	case pubserialization.FieldKindNullableInt16:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt16()
		}, "Int16").(int16)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pubserialization.FieldKindInt32:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	case pubserialization.FieldKindNullableInt32:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt32()
		}, "Int32").(int32)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt64(fieldName string) int64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pubserialization.FieldKindInt64:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadInt64AtPosition(position)
	case pubserialization.FieldKindNullableInt64:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt64()
		}, "Int64").(int64)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat32(fieldName string) float32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pubserialization.FieldKindFloat32:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadFloat32AtPosition(position)
	case pubserialization.FieldKindNullableFloat32:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat32()
		}, "Float32").(float32)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat64(fieldName string) float64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pubserialization.FieldKindFloat64:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadFloat64AtPosition(position)
	case pubserialization.FieldKindNullableFloat64:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat64()
		}, "Float64").(float64)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) *string {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindString)
	value := r.readVariableSizeField(fd, func(in *ObjectDataInput) interface{} {
		str := in.ReadString()
		return &str
	})
	if value == nil {
		return nil
	}
	return value.(*string)
}

func (r DefaultCompactReader) ReadDecimal(fieldName string) *types.Decimal {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindDecimal)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		dec := ReadDecimal(inp)
		return &dec
	}).(*types.Decimal)
}

func (r DefaultCompactReader) ReadTime(fieldName string) *types.LocalTime {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTime)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		time := types.LocalTime(ReadTime(inp))
		return &time
	}).(*types.LocalTime)
}

func (r DefaultCompactReader) ReadDate(fieldName string) *types.LocalDate {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindDate)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		date := types.LocalDate(ReadDate(inp))
		return &date
	}).(*types.LocalDate)
}

func (r DefaultCompactReader) ReadTimestamp(fieldName string) *types.LocalDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTimestamp)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		timestamp := types.LocalDateTime(ReadTimestamp(inp))
		return &timestamp
	}).(*types.LocalDateTime)
}

func (r DefaultCompactReader) ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTimestampWithTimezone)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		timestampWithTimezone := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
		a := time.Time(timestampWithTimezone).String()
		println(a)
		return &timestampWithTimezone
	}).(*types.OffsetDateTime)
}

func (r DefaultCompactReader) ReadCompact(fieldName string) interface{} {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindCompact)
	return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		return r.serializer.Read(r.in)
	})
}

func (r DefaultCompactReader) ReadArrayOfBoolean(fieldName string) []bool {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pubserialization.FieldKindArrayOfBoolean:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			return r.readBooleanBits(inp)
		}).([]bool)
	case pubserialization.FieldKindArrayOfNullableBoolean:
		return r.readNullableArrayAsPrimitiveArray(fd, func(inp *ObjectDataInput) interface{} {
			return r.readBooleanBits(inp)
		}, "Boolean").([]bool)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfInt8(fieldName string) []int8 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt8Array()
	}, pubserialization.FieldKindArrayOfInt8, pubserialization.FieldKindArrayOfNullableInt8, "Int8").([]int8)
}

func (r DefaultCompactReader) ReadArrayOfInt16(fieldName string) []int16 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt16Array()
	}, pubserialization.FieldKindArrayOfInt16, pubserialization.FieldKindArrayOfNullableInt16, "Int16").([]int16)
}

func (r DefaultCompactReader) ReadArrayOfInt32(fieldName string) []int32 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt32Array()
	}, pubserialization.FieldKindArrayOfInt32, pubserialization.FieldKindArrayOfNullableInt32, "Int32").([]int32)
}

func (r DefaultCompactReader) ReadArrayOfInt64(fieldName string) []int64 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt64Array()
	}, pubserialization.FieldKindArrayOfInt64, pubserialization.FieldKindArrayOfNullableInt64, "Int64").([]int64)
}

func (r DefaultCompactReader) ReadArrayOfFloat32(fieldName string) []float32 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadFloat32Array()
	}, pubserialization.FieldKindArrayOfFloat32, pubserialization.FieldKindArrayOfNullableFloat32, "Float32").([]float32)
}

func (r DefaultCompactReader) ReadArrayOfFloat64(fieldName string) []float64 {
	return r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadFloat64Array()
	}, pubserialization.FieldKindArrayOfFloat64, pubserialization.FieldKindArrayOfNullableFloat64, "Float64").([]float64)
}

func (r DefaultCompactReader) ReadArrayOfString(fieldName string) []*string {
	var values []*string
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfString, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			str := inp.ReadString()
			values[i] = &str
		}
	}, func(i int) {
		values = make([]*string, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfDecimal(fieldName string) []*types.Decimal {
	var values []*types.Decimal
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDecimal, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			dec := ReadDecimal(inp)
			values[i] = &dec
		}
	}, func(i int) {
		values = make([]*types.Decimal, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfTime(fieldName string) []*types.LocalTime {
	var values []*types.LocalTime
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTime, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			lt := types.LocalTime(ReadTime(inp))
			values[i] = &lt
		}
	}, func(i int) {
		values = make([]*types.LocalTime, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfDate(fieldName string) []*types.LocalDate {
	var values []*types.LocalDate
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDate, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			ld := types.LocalDate(ReadDate(inp))
			values[i] = &ld
		}
	}, func(i int) {
		values = make([]*types.LocalDate, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfTimestamp(fieldName string) []*types.LocalDateTime {
	var values []*types.LocalDateTime
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestamp, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			ldt := types.LocalDateTime(ReadTimestamp(inp))
			values[i] = &ldt
		}
	}, func(i int) {
		values = make([]*types.LocalDateTime, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfTimestampWithTimezone(fieldName string) []*types.OffsetDateTime {
	var values []*types.OffsetDateTime
	r.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestampWithTimezone, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			odt := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
			values[i] = &odt
		}
	}, func(i int) {
		values = make([]*types.OffsetDateTime, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfCompact(fieldName string) []interface{} {
	reader := func(inp *ObjectDataInput) interface{} {
		return r.serializer.Read(inp)
	}
	fd := r.getFieldDefinition(fieldName)
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	pos := r.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return nil
	}
	r.in.SetPosition(pos)
	dataLength := r.in.readInt32()
	itemCount := r.in.readInt32()
	dataStartPosition := r.in.position
	values := make([]interface{}, itemCount)
	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i++ {
		offset := offsetReader.getOffset(r.in, offsetsPosition, i)
		if offset != nilArrayLength {
			r.in.SetPosition(offset + dataStartPosition)
			values[i] = reader(r.in)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadNullableBoolean(fieldName string) *bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindBoolean:
		b := r.readBoolean(fd)
		return &b
	case pubserialization.FieldKindNullableBoolean:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadBool()
			return &b
		}).(*bool)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt8(fieldName string) *int8 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt8:
		b := r.in.ReadSignedByteAtPosition(r.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt8:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadSignedByte()
			return &b
		}).(*int8)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt16(fieldName string) *int16 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt16:
		b := r.in.ReadInt16AtPosition(r.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt16:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			short := inp.ReadInt16()
			return &short
		}).(*int16)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt32(fieldName string) *int32 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt32:
		b := r.in.ReadInt32AtPosition(r.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt32:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			i := inp.readInt32()
			return &i
		}).(*int32)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt64(fieldName string) *int64 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt64:
		long := r.in.ReadInt64AtPosition(r.getFixedSizePosition(fd))
		return &long
	case pubserialization.FieldKindNullableInt64:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			long := inp.ReadInt64()
			return &long
		}).(*int64)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableFloat32(fieldName string) *float32 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindFloat32:
		f := r.in.ReadFloat32AtPosition(r.getFixedSizePosition(fd))
		return &f
	case pubserialization.FieldKindNullableFloat32:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat32()
			return &f
		}).(*float32)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableFloat64(fieldName string) *float64 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindFloat64:
		f := r.in.ReadFloat64AtPosition(r.getFixedSizePosition(fd))
		return &f
	case pubserialization.FieldKindNullableFloat64:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat64()
			return &f
		}).(*float64)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfNullableBoolean(fieldName string) []*bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindArrayOfBoolean:
		return r.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			return r.readBooleanBitsAsNullables(r.in)
		}).([]*bool)
	case pubserialization.FieldKindArrayOfNullableBoolean:
		var values []*bool
		r.readArrayOfVariableSize(fieldName, fd.fieldKind, func(inp *ObjectDataInput, i int32, isNil bool) {
			if isNil {
				values[i] = nil
			} else {
				b := inp.readBool()
				values[i] = &b
			}
		}, func(i int) {
			values = make([]*bool, i)
		})
		return values
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfNullableInt8(fieldName string) []*int8 {
	var values []*int8
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt8, pubserialization.FieldKindArrayOfNullableInt8, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			b := inp.ReadSignedByte()
			values[i] = &b
		}
	}, func(i int) {
		values = make([]*int8, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt16(fieldName string) []*int16 {
	var values []*int16
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt16, pubserialization.FieldKindArrayOfNullableInt16, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			v := inp.ReadInt16()
			values[i] = &v
		}
	}, func(i int) {
		values = make([]*int16, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt32(fieldName string) []*int32 {
	var values []*int32
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt32, pubserialization.FieldKindArrayOfNullableInt32, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			v := inp.ReadInt32()
			values[i] = &v
		}
	}, func(i int) {
		values = make([]*int32, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt64(fieldName string) []*int64 {
	var values []*int64
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt64, pubserialization.FieldKindArrayOfNullableInt64, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			long := inp.ReadInt64()
			values[i] = &long
		}
	}, func(i int) {
		values = make([]*int64, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat32(fieldName string) []*float32 {
	var values []*float32
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfFloat32, pubserialization.FieldKindArrayOfNullableFloat32, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			f := inp.ReadFloat32()
			values[i] = &f
		}
	}, func(i int) {
		values = make([]*float32, i)
	})
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat64(fieldName string) []*float64 {
	var values []*float64
	r.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfFloat64, pubserialization.FieldKindArrayOfNullableFloat64, func(inp *ObjectDataInput, i int32, isNil bool) {
		if isNil {
			values[i] = nil
		} else {
			f := inp.ReadFloat64()
			values[i] = &f
		}
	}, func(i int) {
		values = make([]*float64, i)
	})
	return values
}

func (r DefaultCompactReader) GetFieldKind(fieldName string) pubserialization.FieldKind {
	field := r.schema.GetField(fieldName)
	if field == nil {
		return pubserialization.FieldKindNotAvailable
	}
	return field.fieldKind
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(newUnknownField(fieldName, r.schema))
	}
	return *fd
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	fd := r.getFieldDefinition(fieldName)
	if fd.fieldKind != fieldKind {
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
	return fd
}

func (r *DefaultCompactReader) getFixedSizePosition(fd FieldDescriptor) int32 {
	return fd.offset + r.startPos
}

func newUnknownField(fieldName string, schema Schema) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unknown field name '%s' for %s", fieldName, schema), nil)
}

func newUnexpectedFieldKind(actualFieldKind pubserialization.FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}

func (r *DefaultCompactReader) unexpectedNullValue(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via read%s methods. Use readNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (r *DefaultCompactReader) unexpectedNullValueInArray(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via readArrayOf%s methods. Use readArrayOfNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (r *DefaultCompactReader) readVariableSizeField(fd FieldDescriptor, reader Reader) interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == nullOffset {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeSlice(fd FieldDescriptor, reader Reader) interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == nullOffset {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeAsNonNull(fd FieldDescriptor, reader Reader, methodSuffix string) interface{} {
	value := r.readVariableSizeField(fd, reader)
	if value == nil {
		panic(r.unexpectedNullValue(fd.fieldName, methodSuffix))
	}
	return value
}

func (r *DefaultCompactReader) readVariableSizeFieldPosition(fd FieldDescriptor) int32 {
	index := fd.index
	offset := r.offsetReader.getOffset(r.in, r.offsetsPos, index)
	if offset == nullOffset {
		return nullOffset
	}
	return offset + r.startPos
}

func (r *DefaultCompactReader) readBooleanBits(inp *ObjectDataInput) []bool {
	len := inp.ReadInt32()
	if len == nilArrayLength {
		return nil
	}
	if len == 0 {
		return make([]bool, 0)
	}
	values := make([]bool, len)
	index := 0
	currentByte := inp.ReadByte()
	for i := int32(0); i < len; i++ {
		if index == BitsInAByte {
			index = 0
			currentByte = inp.ReadByte()
		}
		result := ((currentByte >> index) & 1) != 0
		index += 1
		values[i] = result
	}
	return values
}

func (r *DefaultCompactReader) readBooleanBitsAsNullables(inp *ObjectDataInput) []*bool {
	len := inp.ReadInt32()
	if len == nilArrayLength {
		return nil
	}
	if len == 0 {
		return make([]*bool, 0)
	}
	values := make([]*bool, len)
	index := 0
	currentByte := inp.ReadByte()
	for i := int32(0); i < len; i++ {
		if index == BitsInAByte {
			index = 0
			currentByte = inp.ReadByte()
		}
		result := ((currentByte >> index) & 1) != 0
		index += 1
		values[i] = &result
	}
	return values
}

func (r *DefaultCompactReader) readNullableArrayAsPrimitiveArray(fd FieldDescriptor, reader Reader, methodSuffix string) interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	pos := r.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return nil
	}
	r.in.SetPosition(pos)
	dataLength := r.in.readInt32()
	itemCount := r.in.readInt32()
	dataStartPosition := r.in.position

	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i += 1 {
		offset := offsetReader.getOffset(r.in, offsetsPosition, i)
		if offset == nilArrayLength {
			panic(r.unexpectedNullValueInArray(fd.fieldName, methodSuffix))
		}
	}
	r.in.SetPosition(dataStartPosition - Int32SizeInBytes)
	return reader(r.in)
}

func (r *DefaultCompactReader) readArrayOfPrimitive(fieldName string, reader Reader, primitiveKind, nullableKind pubserialization.FieldKind, methodSuffix string) interface{} {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	if fieldKind == primitiveKind {
		return r.readVariableSizeSlice(fd, reader)
	} else if fieldKind == nullableKind {
		return r.readNullableArrayAsPrimitiveArray(fd, reader, methodSuffix)
	} else {
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r *DefaultCompactReader) readArrayOfVariableSize(fieldName string, fieldKind pubserialization.FieldKind, reader SliceReader, sc SliceConstructor) {
	fd := r.getFieldDefinition(fieldName)
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	pos := r.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return
	}
	r.in.SetPosition(pos)
	dataLength := r.in.readInt32()
	itemCount := r.in.readInt32()
	dataStartPosition := r.in.position
	sc(int(itemCount))
	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i++ {
		offset := offsetReader.getOffset(r.in, offsetsPosition, i)
		if offset != nilArrayLength {
			r.in.SetPosition(offset + dataStartPosition)
			reader(r.in, i, false)
		} else {
			reader(nil, i, true)
		}
	}
}

func (r *DefaultCompactReader) readArrayOfNullable(fieldName string, primitiveKind, nullableKind pubserialization.FieldKind, sr SliceReader, sc SliceConstructor) {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	if fieldKind == primitiveKind {
		r.readPrimitiveArrayAsNullableArray(fd, sr, sc)
	} else if fieldKind == nullableKind {
		r.readArrayOfVariableSize(fieldName, fieldKind, sr, sc)
	} else {
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r *DefaultCompactReader) readPrimitiveArrayAsNullableArray(fd FieldDescriptor, sr SliceReader, sc SliceConstructor) {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	pos := r.readVariableSizeFieldPosition(fd)
	if pos == nullOffset {
		return
	}
	r.in.SetPosition(pos)
	itemCount := r.in.readInt32()
	sc(int(itemCount))
	for i := int32(0); i < itemCount; i++ {
		sr(r.in, i, false)
	}
}

func (r *DefaultCompactReader) readBoolean(fd FieldDescriptor) bool {
	booleanOffset := fd.offset
	bitOffset := fd.bitOffset
	getOffset := booleanOffset + r.startPos
	lastByte := r.in.ReadByteAtPosition(getOffset)
	return ((lastByte >> byte(bitOffset)) & 1) != 0
}

func getOffsetReader(dataLength int32) OffsetReader {
	if dataLength < byteOffsetReaderRange {
		return byteOffsetReader
	} else if dataLength < shortOffsetReaderRange {
		return shortOffsetReader
	} else {
		return intOffsetReader
	}
}
