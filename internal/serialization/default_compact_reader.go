/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

func (ByteOffsetReader) getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32 {
	offset := input.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return nullOffset
	}
	return int32(offset)
}

type ShortOffsetReader struct{}

func (ShortOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos, index int32) int32 {
	offset := inp.ReadInt16AtPosition(variableOffsetsPos + (index * Int16SizeInBytes))
	return int32(offset)
}

type IntOffsetReader struct{}

func (IntOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos, index int32) int32 {
	return inp.ReadInt32AtPosition(variableOffsetsPos + (index * Int32SizeInBytes))
}

type DefaultCompactReader struct {
	offsetReader OffsetReader
	in           *ObjectDataInput
	schema       *Schema
	serializer   CompactStreamSerializer
	startPos     int32
	offsetsPos   int32
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema *Schema) *DefaultCompactReader {
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
	return &DefaultCompactReader{
		schema:       schema,
		in:           input,
		serializer:   serializer,
		startPos:     startPos,
		offsetReader: offsetReader,
		offsetsPos:   varOffsetsPos,
	}
}

func (d *DefaultCompactReader) ReadBoolean(fieldName string) bool {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindBoolean:
		return d.readBoolean(fd)
	case pubserialization.FieldKindNullableBoolean:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadBool()
		}, "Boolean").(bool)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadInt8(fieldName string) int8 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt8:
		return d.in.ReadSignedByteAtPosition(d.getFixedSizePosition(fd))
	case pubserialization.FieldKindNullableInt8:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadSignedByte()
		}, "Int8").(int8)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadInt16(fieldName string) int16 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt16:
		return d.in.ReadInt16AtPosition(d.getFixedSizePosition(fd))
	case pubserialization.FieldKindNullableInt16:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt16()
		}, "Int16").(int16)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	switch fieldKind {
	case pubserialization.FieldKindInt32:
		position := d.getFixedSizePosition(fd)
		return d.in.ReadInt32AtPosition(position)
	case pubserialization.FieldKindNullableInt32:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt32()
		}, "Int32").(int32)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadInt64(fieldName string) int64 {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	switch fieldKind {
	case pubserialization.FieldKindInt64:
		position := d.getFixedSizePosition(fd)
		return d.in.ReadInt64AtPosition(position)
	case pubserialization.FieldKindNullableInt64:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt64()
		}, "Int64").(int64)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadFloat32(fieldName string) float32 {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	switch fieldKind {
	case pubserialization.FieldKindFloat32:
		position := d.getFixedSizePosition(fd)
		return d.in.ReadFloat32AtPosition(position)
	case pubserialization.FieldKindNullableFloat32:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat32()
		}, "Float32").(float32)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadFloat64(fieldName string) float64 {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	switch fieldKind {
	case pubserialization.FieldKindFloat64:
		position := d.getFixedSizePosition(fd)
		return d.in.ReadFloat64AtPosition(position)
	case pubserialization.FieldKindNullableFloat64:
		return d.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat64()
		}, "Float64").(float64)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadString(fieldName string) *string {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindString)
	value := d.readVariableSizeField(fd, func(in *ObjectDataInput) interface{} {
		str := in.ReadString()
		return &str
	})
	if value == nil {
		return nil
	}
	return value.(*string)
}

func (d *DefaultCompactReader) ReadDecimal(fieldName string) *types.Decimal {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindDecimal)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		dec := ReadDecimal(inp)
		return &dec
	}).(*types.Decimal)
}

func (d *DefaultCompactReader) ReadTime(fieldName string) *types.LocalTime {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTime)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		time := types.LocalTime(ReadTime(inp))
		return &time
	}).(*types.LocalTime)
}

func (d *DefaultCompactReader) ReadDate(fieldName string) *types.LocalDate {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindDate)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		date := types.LocalDate(ReadDate(inp))
		return &date
	}).(*types.LocalDate)
}

func (d *DefaultCompactReader) ReadTimestamp(fieldName string) *types.LocalDateTime {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTimestamp)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		timestamp := types.LocalDateTime(ReadTimestamp(inp))
		return &timestamp
	}).(*types.LocalDateTime)
}

func (d *DefaultCompactReader) ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindTimestampWithTimezone)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		timestampWithTimezone := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
		a := time.Time(timestampWithTimezone).String()
		println(a)
		return &timestampWithTimezone
	}).(*types.OffsetDateTime)
}

func (d *DefaultCompactReader) ReadCompact(fieldName string) interface{} {
	fd := d.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindCompact)
	return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
		return d.serializer.Read(d.in)
	})
}

func (d *DefaultCompactReader) ReadArrayOfBoolean(fieldName string) []bool {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	switch fieldKind {
	case pubserialization.FieldKindArrayOfBoolean:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			return d.readBooleanBits(inp)
		}).([]bool)
	case pubserialization.FieldKindArrayOfNullableBoolean:
		return d.readNullableArrayAsPrimitiveArray(fd, func(inp *ObjectDataInput) interface{} {
			return d.readBooleanBits(inp)
		}, "Boolean").([]bool)
	default:
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadArrayOfInt8(fieldName string) []int8 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt8Array()
	}, pubserialization.FieldKindArrayOfInt8, pubserialization.FieldKindArrayOfNullableInt8, "Int8").([]int8)
}

func (d *DefaultCompactReader) ReadArrayOfInt16(fieldName string) []int16 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt16Array()
	}, pubserialization.FieldKindArrayOfInt16, pubserialization.FieldKindArrayOfNullableInt16, "Int16").([]int16)
}

func (d *DefaultCompactReader) ReadArrayOfInt32(fieldName string) []int32 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt32Array()
	}, pubserialization.FieldKindArrayOfInt32, pubserialization.FieldKindArrayOfNullableInt32, "Int32").([]int32)
}

func (d *DefaultCompactReader) ReadArrayOfInt64(fieldName string) []int64 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadInt64Array()
	}, pubserialization.FieldKindArrayOfInt64, pubserialization.FieldKindArrayOfNullableInt64, "Int64").([]int64)
}

func (d *DefaultCompactReader) ReadArrayOfFloat32(fieldName string) []float32 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadFloat32Array()
	}, pubserialization.FieldKindArrayOfFloat32, pubserialization.FieldKindArrayOfNullableFloat32, "Float32").([]float32)
}

func (d *DefaultCompactReader) ReadArrayOfFloat64(fieldName string) []float64 {
	return d.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) interface{} {
		return inp.ReadFloat64Array()
	}, pubserialization.FieldKindArrayOfFloat64, pubserialization.FieldKindArrayOfNullableFloat64, "Float64").([]float64)
}

func (d *DefaultCompactReader) ReadArrayOfString(fieldName string) []*string {
	var values []*string
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfString, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			str := inp.ReadString()
			values[i] = &str
		}
	}, func(i int) {
		values = make([]*string, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfDecimal(fieldName string) []*types.Decimal {
	var values []*types.Decimal
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDecimal, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			dec := ReadDecimal(inp)
			values[i] = &dec
		}
	}, func(i int) {
		values = make([]*types.Decimal, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfTime(fieldName string) []*types.LocalTime {
	var values []*types.LocalTime
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTime, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			lt := types.LocalTime(ReadTime(inp))
			values[i] = &lt
		}
	}, func(i int) {
		values = make([]*types.LocalTime, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfDate(fieldName string) []*types.LocalDate {
	var values []*types.LocalDate
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDate, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			ld := types.LocalDate(ReadDate(inp))
			values[i] = &ld
		}
	}, func(i int) {
		values = make([]*types.LocalDate, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfTimestamp(fieldName string) []*types.LocalDateTime {
	var values []*types.LocalDateTime
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestamp, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			ldt := types.LocalDateTime(ReadTimestamp(inp))
			values[i] = &ldt
		}
	}, func(i int) {
		values = make([]*types.LocalDateTime, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfTimestampWithTimezone(fieldName string) []*types.OffsetDateTime {
	var values []*types.OffsetDateTime
	d.readArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestampWithTimezone, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			odt := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
			values[i] = &odt
		}
	}, func(i int) {
		values = make([]*types.OffsetDateTime, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfCompact(fieldName string) []interface{} {
	reader := func(inp *ObjectDataInput) interface{} {
		return d.serializer.Read(inp)
	}
	fd := d.getFieldDefinition(fieldName)
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	pos := d.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return nil
	}
	d.in.SetPosition(pos)
	dataLength := d.in.readInt32()
	itemCount := d.in.readInt32()
	dataStartPosition := d.in.position
	values := make([]interface{}, itemCount)
	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i++ {
		offset := offsetReader.getOffset(d.in, offsetsPosition, i)
		if offset != nilArrayLength {
			d.in.SetPosition(offset + dataStartPosition)
			values[i] = reader(d.in)
		}
	}
	return values
}

func (d *DefaultCompactReader) ReadNullableBoolean(fieldName string) *bool {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindBoolean:
		b := d.readBoolean(fd)
		return &b
	case pubserialization.FieldKindNullableBoolean:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadBool()
			return &b
		}).(*bool)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableInt8(fieldName string) *int8 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt8:
		b := d.in.ReadSignedByteAtPosition(d.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt8:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadSignedByte()
			return &b
		}).(*int8)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableInt16(fieldName string) *int16 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt16:
		b := d.in.ReadInt16AtPosition(d.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt16:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			short := inp.ReadInt16()
			return &short
		}).(*int16)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableInt32(fieldName string) *int32 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt32:
		b := d.in.ReadInt32AtPosition(d.getFixedSizePosition(fd))
		return &b
	case pubserialization.FieldKindNullableInt32:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			i := inp.readInt32()
			return &i
		}).(*int32)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableInt64(fieldName string) *int64 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindInt64:
		long := d.in.ReadInt64AtPosition(d.getFixedSizePosition(fd))
		return &long
	case pubserialization.FieldKindNullableInt64:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			long := inp.ReadInt64()
			return &long
		}).(*int64)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableFloat32(fieldName string) *float32 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindFloat32:
		f := d.in.ReadFloat32AtPosition(d.getFixedSizePosition(fd))
		return &f
	case pubserialization.FieldKindNullableFloat32:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat32()
			return &f
		}).(*float32)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadNullableFloat64(fieldName string) *float64 {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindFloat64:
		f := d.in.ReadFloat64AtPosition(d.getFixedSizePosition(fd))
		return &f
	case pubserialization.FieldKindNullableFloat64:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat64()
			return &f
		}).(*float64)
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadArrayOfNullableBoolean(fieldName string) []*bool {
	fd := d.getFieldDefinition(fieldName)
	switch fd.Kind {
	case pubserialization.FieldKindArrayOfBoolean:
		return d.readVariableSizeField(fd, func(inp *ObjectDataInput) interface{} {
			return d.readBooleanBitsAsNullables(d.in)
		}).([]*bool)
	case pubserialization.FieldKindArrayOfNullableBoolean:
		var values []*bool
		d.readArrayOfVariableSize(fieldName, fd.Kind, func(inp *ObjectDataInput, i int32, isNil bool) {
			if !isNil {
				b := inp.readBool()
				values[i] = &b
			}
		}, func(i int) {
			values = make([]*bool, i)
		})
		return values
	default:
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
}

func (d *DefaultCompactReader) ReadArrayOfNullableInt8(fieldName string) []*int8 {
	var values []*int8
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt8, pubserialization.FieldKindArrayOfNullableInt8, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			b := inp.ReadSignedByte()
			values[i] = &b
		}
	}, func(i int) {
		values = make([]*int8, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfNullableInt16(fieldName string) []*int16 {
	var values []*int16
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt16, pubserialization.FieldKindArrayOfNullableInt16, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			v := inp.ReadInt16()
			values[i] = &v
		}
	}, func(i int) {
		values = make([]*int16, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfNullableInt32(fieldName string) []*int32 {
	var values []*int32
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt32, pubserialization.FieldKindArrayOfNullableInt32, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			v := inp.ReadInt32()
			values[i] = &v
		}
	}, func(i int) {
		values = make([]*int32, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfNullableInt64(fieldName string) []*int64 {
	var values []*int64
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfInt64, pubserialization.FieldKindArrayOfNullableInt64, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			long := inp.ReadInt64()
			values[i] = &long
		}
	}, func(i int) {
		values = make([]*int64, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfNullableFloat32(fieldName string) []*float32 {
	var values []*float32
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfFloat32, pubserialization.FieldKindArrayOfNullableFloat32, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			f := inp.ReadFloat32()
			values[i] = &f
		}
	}, func(i int) {
		values = make([]*float32, i)
	})
	return values
}

func (d *DefaultCompactReader) ReadArrayOfNullableFloat64(fieldName string) []*float64 {
	var values []*float64
	d.readArrayOfNullable(fieldName, pubserialization.FieldKindArrayOfFloat64, pubserialization.FieldKindArrayOfNullableFloat64, func(inp *ObjectDataInput, i int32, isNil bool) {
		if !isNil {
			f := inp.ReadFloat64()
			values[i] = &f
		}
	}, func(i int) {
		values = make([]*float64, i)
	})
	return values
}

func (d *DefaultCompactReader) GetFieldKind(fieldName string) pubserialization.FieldKind {
	field := d.schema.GetField(fieldName)
	if field == nil {
		return pubserialization.FieldKindNotAvailable
	}
	return field.Kind
}

func (d *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := d.schema.GetField(fieldName)
	if fd == nil {
		panic(newUnknownField(fieldName, d.schema))
	}
	return *fd
}

func (d *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	fd := d.getFieldDefinition(fieldName)
	if fd.Kind != fieldKind {
		panic(newUnexpectedFieldKind(fd.Kind, fieldName))
	}
	return fd
}

func (d *DefaultCompactReader) getFixedSizePosition(fd FieldDescriptor) int32 {
	return fd.offset + d.startPos
}

func (d *DefaultCompactReader) unexpectedNullValue(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via read%s methods. Use readNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (d *DefaultCompactReader) unexpectedNullValueInArray(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via readArrayOf%s methods. Use readArrayOfNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (d *DefaultCompactReader) readVariableSizeField(fd FieldDescriptor, r Reader) interface{} {
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	position := d.readVariableSizeFieldPosition(fd)
	if position == nullOffset {
		return nil
	}
	d.in.SetPosition(position)
	return r(d.in)
}

func (d *DefaultCompactReader) readVariableSizeSlice(fd FieldDescriptor, r Reader) interface{} {
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	position := d.readVariableSizeFieldPosition(fd)
	if position == nullOffset {
		return nil
	}
	d.in.SetPosition(position)
	return r(d.in)
}

func (d *DefaultCompactReader) readVariableSizeAsNonNull(fd FieldDescriptor, r Reader, methodSuffix string) interface{} {
	value := d.readVariableSizeField(fd, r)
	if value == nil {
		panic(d.unexpectedNullValue(fd.FieldName, methodSuffix))
	}
	return value
}

func (d *DefaultCompactReader) readVariableSizeFieldPosition(fd FieldDescriptor) int32 {
	index := fd.index
	offset := d.offsetReader.getOffset(d.in, d.offsetsPos, index)
	if offset == nullOffset {
		return nullOffset
	}
	return offset + d.startPos
}

func (d *DefaultCompactReader) readBooleanBits(inp *ObjectDataInput) []bool {
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

func (d *DefaultCompactReader) readBooleanBitsAsNullables(inp *ObjectDataInput) []*bool {
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

func (d *DefaultCompactReader) readNullableArrayAsPrimitiveArray(fd FieldDescriptor, r Reader, methodSuffix string) interface{} {
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	pos := d.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return nil
	}
	d.in.SetPosition(pos)
	dataLength := d.in.readInt32()
	itemCount := d.in.readInt32()
	dataStartPosition := d.in.position

	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i += 1 {
		offset := offsetReader.getOffset(d.in, offsetsPosition, i)
		if offset == nilArrayLength {
			panic(d.unexpectedNullValueInArray(fd.FieldName, methodSuffix))
		}
	}
	d.in.SetPosition(dataStartPosition - Int32SizeInBytes)
	return r(d.in)
}

func (d *DefaultCompactReader) readArrayOfPrimitive(fieldName string, r Reader, primitiveKind, nullableKind pubserialization.FieldKind, methodSuffix string) interface{} {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	if fieldKind == primitiveKind {
		return d.readVariableSizeSlice(fd, r)
	} else if fieldKind == nullableKind {
		return d.readNullableArrayAsPrimitiveArray(fd, r, methodSuffix)
	} else {
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) readArrayOfVariableSize(fieldName string, fieldKind pubserialization.FieldKind, sr SliceReader, sc SliceConstructor) {
	fd := d.getFieldDefinition(fieldName)
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	pos := d.readVariableSizeFieldPosition(fd)
	if pos == nilArrayLength {
		return
	}
	d.in.SetPosition(pos)
	dataLength := d.in.readInt32()
	itemCount := d.in.readInt32()
	dataStartPosition := d.in.position
	sc(int(itemCount))
	offsetReader := getOffsetReader(dataLength)
	offsetsPosition := dataStartPosition + dataLength
	for i := int32(0); i < itemCount; i++ {
		offset := offsetReader.getOffset(d.in, offsetsPosition, i)
		if offset != nilArrayLength {
			d.in.SetPosition(offset + dataStartPosition)
			sr(d.in, i, false)
		} else {
			sr(nil, i, true)
		}
	}
}

func (d *DefaultCompactReader) readArrayOfNullable(fieldName string, primitiveKind, nullableKind pubserialization.FieldKind, sr SliceReader, sc SliceConstructor) {
	fd := d.getFieldDefinition(fieldName)
	fieldKind := fd.Kind
	if fieldKind == primitiveKind {
		d.readPrimitiveArrayAsNullableArray(fd, sr, sc)
	} else if fieldKind == nullableKind {
		d.readArrayOfVariableSize(fieldName, fieldKind, sr, sc)
	} else {
		panic(newUnexpectedFieldKind(fieldKind, fieldName))
	}
}

func (d *DefaultCompactReader) readPrimitiveArrayAsNullableArray(fd FieldDescriptor, sr SliceReader, sc SliceConstructor) {
	currentPos := d.in.position
	defer d.in.SetPosition(currentPos)
	pos := d.readVariableSizeFieldPosition(fd)
	if pos == nullOffset {
		return
	}
	d.in.SetPosition(pos)
	itemCount := d.in.readInt32()
	sc(int(itemCount))
	for i := int32(0); i < itemCount; i++ {
		sr(d.in, i, false)
	}
}

func (d *DefaultCompactReader) readBoolean(fd FieldDescriptor) bool {
	booleanOffset := fd.offset
	bitOffset := fd.bitOffset
	getOffset := booleanOffset + d.startPos
	lastByte := d.in.ReadByteAtPosition(getOffset)
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

func newUnknownField(fieldName string, schema *Schema) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unknown field name '%s' for %s", fieldName, schema), nil)
}

func newUnexpectedFieldKind(actualFieldKind pubserialization.FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}
