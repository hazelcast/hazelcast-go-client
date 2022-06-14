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
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	NULL_ARRAY_LENGTH         = -1
	NULL_OFFSET               = -1
	BYTE_OFFSET_READER_RANGE  = 127 - (-128)
	SHORT_OFFSET_READER_RANGE = 32767 - (-32768)
)

type Reader func(*ObjectDataInput) interface{}
type SliceReader func(*ObjectDataInput) []interface{}
type SliceConstructor func(int32) interface{}

var BYTE_OFFSET_READER = ByteOffsetReader{}
var SHORT_OFFSET_READER = ShortOffsetReader{}
var INT_OFFSET_READER = IntOffsetReader{}

type OffsetReader interface {
	getOffset(input *ObjectDataInput, variableOffsetsPos int32, index int32) int32
}

type ByteOffsetReader struct{}

func (ByteOffsetReader) getOffset(inp *ObjectDataInput, variableOffsetsPos int32, index int32) int32 {
	offset := inp.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return NULL_OFFSET
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
	offsetReader            OffsetReader
	in                      *ObjectDataInput
	serializer              CompactStreamSerializer
	schema                  Schema
	dataStartPosition       int32
	variableOffsetsPosition int32
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	numberOfVarSizeFields := schema.numberOfVarSizeFields

	var variableOffsetsPosition, dataStartPosition, finalPosition int32
	var offsetReader OffsetReader

	if numberOfVarSizeFields == 0 {
		offsetReader = INT_OFFSET_READER
		variableOffsetsPosition = 0
		dataStartPosition = input.position
		finalPosition = dataStartPosition + schema.fixedSizeFieldsLength
	} else {
		dataLength := input.readInt32()
		dataStartPosition = input.position
		variableOffsetsPosition = dataStartPosition + dataLength
		if dataLength < BYTE_OFFSET_READER_RANGE {
			offsetReader = BYTE_OFFSET_READER
			finalPosition = variableOffsetsPosition + numberOfVarSizeFields
		} else if dataLength < SHORT_OFFSET_READER_RANGE {
			offsetReader = SHORT_OFFSET_READER
			finalPosition = variableOffsetsPosition + numberOfVarSizeFields*Int16SizeInBytes
		} else {
			offsetReader = INT_OFFSET_READER
			finalPosition = variableOffsetsPosition + numberOfVarSizeFields*Int32SizeInBytes
		}
	}
	input.SetPosition(finalPosition)

	return DefaultCompactReader{
		schema:                  schema,
		in:                      input,
		serializer:              serializer,
		dataStartPosition:       dataStartPosition,
		offsetReader:            offsetReader,
		variableOffsetsPosition: variableOffsetsPosition,
	}
}

func (r DefaultCompactReader) ReadBoolean(fieldName string) bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindBoolean:
		return r.readBoolean(fd)
	case pserialization.FieldKindNullableBoolean:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadBool()
		}, "Boolean").(bool)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt8(fieldName string) int8 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt8:
		return r.in.ReadSignedByteAtPosition(r.getFixedSizePosition(fd))
	case pserialization.FieldKindNullableInt8:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadSignedByte()
		}, "Int8").(int8)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt16(fieldName string) int16 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt16:
		return r.in.ReadInt16AtPosition(r.getFixedSizePosition(fd))
	case pserialization.FieldKindNullableInt16:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt16()
		}, "Int16").(int16)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindInt32:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	case pserialization.FieldKindNullableInt32:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt32()
		}, "Int32").(int32)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt64(fieldName string) int64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindInt64:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadInt64AtPosition(position)
	case pserialization.FieldKindNullableInt64:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt64()
		}, "Int64").(int64)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat32(fieldName string) float32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindFloat32:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadFloat32AtPosition(position)
	case pserialization.FieldKindNullableFloat32:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat32()
		}, "Float32").(float32)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat64(fieldName string) float64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindFloat64:
		position := r.getFixedSizePosition(fd)
		return r.in.ReadFloat64AtPosition(position)
	case pserialization.FieldKindNullableFloat64:
		return r.readVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat64()
		}, "Float64").(float64)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) *string {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindString)

	value := r.readVariableSize(fd, func(in *ObjectDataInput) interface{} {
		str := in.ReadString()
		return &str
	})

	if value == nil {
		return nil
	} else {
		return value.(*string)
	}
}

func (r DefaultCompactReader) ReadDecimal(fieldName string) *types.Decimal {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindDecimal)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		dec := ReadDecimal(inp)
		return &dec
	}).(*types.Decimal)
}

func (r DefaultCompactReader) ReadTime(fieldName string) *types.LocalTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTime)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		time := types.LocalTime(ReadTime(inp))
		return &time
	}).(*types.LocalTime)
}

func (r DefaultCompactReader) ReadDate(fieldName string) *types.LocalDate {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindDate)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		date := types.LocalDate(ReadDate(inp))
		return &date
	}).(*types.LocalDate)
}

func (r DefaultCompactReader) ReadTimestamp(fieldName string) *types.LocalDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTimestamp)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		timestamp := types.LocalDateTime(ReadTimestamp(inp))
		return &timestamp
	}).(*types.LocalDateTime)
}

func (r DefaultCompactReader) ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTimestampWithTimezone)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		timestampWithTimezone := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
		a := time.Time(timestampWithTimezone).String()
		println(a)
		return &timestampWithTimezone
	}).(*types.OffsetDateTime)
}

func (r DefaultCompactReader) ReadCompact(fieldName string) interface{} {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindCompact)
	return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return r.serializer.Read(r.in)
	})
}

func (r DefaultCompactReader) ReadArrayOfBoolean(fieldName string) []bool {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindArrayOfBoolean:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			return r.readBooleanBits(inp)
		}).([]bool)
	case pserialization.FieldKindArrayOfNullableBoolean:
		values := r.readNullableArrayAsPrimitiveArray(fd, func(inp *ObjectDataInput) []interface{} {
			values := r.readBooleanBits(inp)
			interfaceValues := make([]interface{}, len(values))
			for i, v := range values {
				interfaceValues[i] = v
			}
			return interfaceValues
		}, "Boolean")
		bools := make([]bool, len(values))
		for i, v := range values {
			bools[i] = v.(bool)
		}
		return bools
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfInt8(fieldName string) []int8 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadInt8Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfInt8, pserialization.FieldKindArrayOfNullableInt8, "Int8")
	values := make([]int8, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(int8)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfInt16(fieldName string) []int16 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadInt16Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfInt16, pserialization.FieldKindArrayOfNullableInt16, "Int16")
	values := make([]int16, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(int16)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfInt32(fieldName string) []int32 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadInt32Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfInt32, pserialization.FieldKindArrayOfNullableInt32, "Int32")
	values := make([]int32, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(int32)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfInt64(fieldName string) []int64 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadInt64Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfInt64, pserialization.FieldKindArrayOfNullableInt64, "Int64")
	values := make([]int64, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(int64)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfFloat32(fieldName string) []float32 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadFloat32Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfFloat32, pserialization.FieldKindArrayOfNullableFloat32, "Float32")
	values := make([]float32, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(float32)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfFloat64(fieldName string) []float64 {
	interfaceValues := r.readArrayOfPrimitive(fieldName, func(inp *ObjectDataInput) []interface{} {
		values := inp.ReadFloat64Array()
		interfaceValues := make([]interface{}, len(values))
		for i, v := range values {
			interfaceValues[i] = v
		}
		return interfaceValues
	}, pserialization.FieldKindArrayOfFloat64, pserialization.FieldKindArrayOfNullableFloat64, "Float64")
	values := make([]float64, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(float64)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfString(fieldName string) []*string {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfString, func(inp *ObjectDataInput) interface{} {
		str := inp.ReadString()
		return &str
	})
	values := make([]*string, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*string)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfDecimal(fieldName string) []*types.Decimal {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfDecimal, func(inp *ObjectDataInput) interface{} {
		dec := ReadDecimal(inp)
		return &dec
	})
	values := make([]*types.Decimal, len(interfaceValues))
	for i, v := range interfaceValues {
		values[i] = v.(*types.Decimal)
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfTime(fieldName string) []*types.LocalTime {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTime, func(inp *ObjectDataInput) interface{} {
		lt := types.LocalTime(ReadTime(inp))
		return &lt
	})
	values := make([]*types.LocalTime, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*types.LocalTime)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfDate(fieldName string) []*types.LocalDate {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfDate, func(inp *ObjectDataInput) interface{} {
		ld := types.LocalDate(ReadDate(inp))
		return &ld
	})
	values := make([]*types.LocalDate, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*types.LocalDate)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfTimestamp(fieldName string) []*types.LocalDateTime {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTimestamp, func(inp *ObjectDataInput) interface{} {
		ldt := types.LocalDateTime(ReadTimestamp(inp))
		return &ldt
	})
	values := make([]*types.LocalDateTime, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*types.LocalDateTime)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfTimestampWithTimezone(fieldName string) []*types.OffsetDateTime {
	interfaceValues := r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTimestampWithTimezone, func(inp *ObjectDataInput) interface{} {
		odt := types.OffsetDateTime(ReadTimestampWithTimezone(inp))
		return &odt
	})
	values := make([]*types.OffsetDateTime, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*types.OffsetDateTime)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfCompact(fieldName string) []interface{} {
	return r.readArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfCompact, func(inp *ObjectDataInput) interface{} {
		return r.serializer.Read(inp)
	})
}

func (r DefaultCompactReader) ReadNullableBoolean(fieldName string) *bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindBoolean:
		b := r.readBoolean(fd)
		return &b
	case pserialization.FieldKindNullableBoolean:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadBool()
			return &b
		}).(*bool)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt8(fieldName string) *int8 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt8:
		b := r.in.ReadSignedByteAtPosition(r.getFixedSizePosition(fd))
		return &b
	case pserialization.FieldKindNullableInt8:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			b := inp.ReadSignedByte()
			return &b
		}).(*int8)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt16(fieldName string) *int16 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt16:
		b := r.in.ReadInt16AtPosition(r.getFixedSizePosition(fd))
		return &b
	case pserialization.FieldKindNullableInt16:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			short := inp.ReadInt16()
			return &short
		}).(*int16)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt32(fieldName string) *int32 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt32:
		b := r.in.ReadInt32AtPosition(r.getFixedSizePosition(fd))
		return &b
	case pserialization.FieldKindNullableInt32:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			i := inp.readInt32()
			return &i
		}).(*int32)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableInt64(fieldName string) *int64 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt64:
		long := r.in.ReadInt64AtPosition(r.getFixedSizePosition(fd))
		return &long
	case pserialization.FieldKindNullableInt64:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			long := inp.ReadInt64()
			return &long
		}).(*int64)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableFloat32(fieldName string) *float32 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindFloat32:
		f := r.in.ReadFloat32AtPosition(r.getFixedSizePosition(fd))
		return &f
	case pserialization.FieldKindNullableFloat32:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat32()
			return &f
		}).(*float32)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadNullableFloat64(fieldName string) *float64 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindFloat64:
		f := r.in.ReadFloat64AtPosition(r.getFixedSizePosition(fd))
		return &f
	case pserialization.FieldKindNullableFloat64:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			f := inp.ReadFloat64()
			return &f
		}).(*float64)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfNullableBoolean(fieldName string) []*bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindArrayOfBoolean:
		return r.readVariableSize(fd, func(inp *ObjectDataInput) interface{} {
			return r.readBooleanBitsAsNullables(r.in)
		}).([]*bool)
	case pserialization.FieldKindArrayOfNullableBoolean:
		interfaceValues := r.readArrayOfVariableSize(fieldName, fd.fieldKind, func(inp *ObjectDataInput) interface{} {
			b := inp.readBool()
			return &b
		})
		values := make([]*bool, len(interfaceValues))
		for i, v := range interfaceValues {
			if v == nil {
				values[i] = nil
			} else {
				values[i] = v.(*bool)
			}
		}
		return values
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadArrayOfNullableInt8(fieldName string) []*int8 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfInt8, pserialization.FieldKindArrayOfNullableInt8, func(inp *ObjectDataInput) interface{} {
		b := inp.ReadSignedByte()
		return &b
	})
	values := make([]*int8, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*int8)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt16(fieldName string) []*int16 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfInt16, pserialization.FieldKindArrayOfNullableInt16, func(inp *ObjectDataInput) interface{} {
		i := inp.ReadInt16()
		return &i
	})
	values := make([]*int16, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*int16)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt32(fieldName string) []*int32 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfInt32, pserialization.FieldKindArrayOfNullableInt32, func(inp *ObjectDataInput) interface{} {
		i := inp.readInt32()
		return &i
	})
	values := make([]*int32, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*int32)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableInt64(fieldName string) []*int64 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfInt64, pserialization.FieldKindArrayOfNullableInt64, func(inp *ObjectDataInput) interface{} {
		long := inp.ReadInt64()
		return &long
	})
	values := make([]*int64, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*int64)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat32(fieldName string) []*float32 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfFloat32, pserialization.FieldKindArrayOfNullableFloat32, func(inp *ObjectDataInput) interface{} {
		f := inp.ReadFloat32()
		return &f
	})
	values := make([]*float32, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*float32)
		}
	}
	return values
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat64(fieldName string) []*float64 {
	interfaceValues := r.readArrayOfNullable(fieldName, pserialization.FieldKindArrayOfFloat64, pserialization.FieldKindArrayOfNullableFloat64, func(inp *ObjectDataInput) interface{} {
		f := inp.ReadFloat64()
		return &f
	})
	values := make([]*float64, len(interfaceValues))
	for i, v := range interfaceValues {
		if v == nil {
			values[i] = nil
		} else {
			values[i] = v.(*float64)
		}
	}
	return values
}

func (r DefaultCompactReader) GetFieldKind(fieldName string) pserialization.FieldKind {
	field := r.schema.GetField(fieldName)
	if field == nil {
		return pserialization.FieldKindNotAvailable
	}
	return field.fieldKind
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(r.unknownField(fieldName))
	}
	return *fd
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind pserialization.FieldKind) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd.fieldKind != fieldKind {
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
	return *fd
}

func (r *DefaultCompactReader) getFixedSizePosition(fd FieldDescriptor) int32 {
	primitiveOffset := fd.offset
	return primitiveOffset + r.dataStartPosition
}

func (r *DefaultCompactReader) unknownField(fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unknown field name '%s' for %s", fieldName, r.schema.ToString()), nil)
}

func (r *DefaultCompactReader) unexpectedFieldKind(actualFieldKind pserialization.FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}

func (r *DefaultCompactReader) unexpectedNullValue(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via read%s methods. Use readNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (r *DefaultCompactReader) unexpectedNullValueInArray(fieldName, methodSuffix string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via readArrayOf%s methods. Use readArrayOfNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (r *DefaultCompactReader) readVariableSize(fd FieldDescriptor, reader Reader) interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == NULL_OFFSET {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeSlice(fd FieldDescriptor, reader SliceReader) []interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == NULL_OFFSET {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeAsNonNull(fd FieldDescriptor, reader Reader, methodSuffix string) interface{} {
	value := r.readVariableSize(fd, reader)
	if value == nil {
		panic(r.unexpectedNullValue(fd.fieldName, methodSuffix))
	}
	return value
}

func (r *DefaultCompactReader) readVariableSizeFieldPosition(fd FieldDescriptor) int32 {
	index := fd.index
	offset := r.offsetReader.getOffset(r.in, r.variableOffsetsPosition, index)
	if offset == NULL_OFFSET {
		return NULL_OFFSET
	} else {
		return offset + r.dataStartPosition
	}
}

func (r *DefaultCompactReader) readBooleanBits(inp *ObjectDataInput) []bool {
	len := inp.ReadInt32()
	if len == NULL_ARRAY_LENGTH {
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
	if len == NULL_ARRAY_LENGTH {
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

func (r *DefaultCompactReader) readNullableArrayAsPrimitiveArray(fd FieldDescriptor, reader SliceReader, methodSuffix string) []interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)

	pos := r.readVariableSizeFieldPosition(fd)
	if pos == NULL_ARRAY_LENGTH {
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
		if offset == NULL_ARRAY_LENGTH {
			panic(r.unexpectedNullValueInArray(fd.fieldName, methodSuffix))
		}
	}
	r.in.SetPosition(dataStartPosition - Int32SizeInBytes)
	return reader(r.in)
}

func (r *DefaultCompactReader) readArrayOfPrimitive(fieldName string, reader SliceReader, primitiveKind, nullableKind pserialization.FieldKind, methodSuffix string) []interface{} {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	if fieldKind == primitiveKind {
		return r.readVariableSizeSlice(fd, reader)
	} else if fieldKind == nullableKind {
		return r.readNullableArrayAsPrimitiveArray(fd, reader, methodSuffix)
	}
	panic(r.unexpectedFieldKind(fieldKind, fieldName))
}

func (r *DefaultCompactReader) readArrayOfVariableSize(fieldName string, fieldKind pserialization.FieldKind, reader Reader) []interface{} {
	fd := r.getFieldDefinition(fieldName)

	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	pos := r.readVariableSizeFieldPosition(fd)
	if pos == NULL_ARRAY_LENGTH {
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
		if offset != NULL_ARRAY_LENGTH {
			r.in.SetPosition(offset + dataStartPosition)
			values[i] = reader(r.in)
		} else {
			values[i] = nil
		}
	}
	return values
}

func (r *DefaultCompactReader) readArrayOfNullable(fieldName string, primitiveKind, nullableKind pserialization.FieldKind, reader Reader) []interface{} {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind

	if fieldKind == primitiveKind {
		return r.readPrimitiveArrayAsNullableArray(fd, reader)
	} else if fieldKind == nullableKind {
		return r.readArrayOfVariableSize(fieldName, fieldKind, reader)
	}
	panic(r.unexpectedFieldKind(fieldKind, fieldName))
}

func (r *DefaultCompactReader) readPrimitiveArrayAsNullableArray(fd FieldDescriptor, reader Reader) []interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)

	pos := r.readVariableSizeFieldPosition(fd)
	if pos == NULL_OFFSET {
		return nil
	}
	r.in.SetPosition(pos)
	itemCount := r.in.readInt32()
	values := make([]interface{}, itemCount)

	for i := int32(0); i < itemCount; i++ {
		values[i] = reader(r.in)
	}

	return values
}

func (r *DefaultCompactReader) readBoolean(fd FieldDescriptor) bool {
	booleanOffset := fd.offset
	bitOffset := fd.bitOffset
	getOffset := booleanOffset + r.dataStartPosition
	lastByte := r.in.ReadByteAtPosition(getOffset)
	return ((lastByte >> byte(bitOffset)) & 1) != 0
}

func getOffsetReader(dataLength int32) OffsetReader {
	if dataLength < BYTE_OFFSET_READER_RANGE {
		return BYTE_OFFSET_READER
	} else if dataLength < SHORT_OFFSET_READER_RANGE {
		return SHORT_OFFSET_READER
	} else {
		return INT_OFFSET_READER
	}
}