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
	"math/big"
	"time"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const NULL_OFFSET = -1

type OffsetReader interface {
	getOffset(input *ObjectDataInput, variableOffsetsPos int32, index int32) int32
}

type ByteOffsetReader struct{}

func (ByteOffsetReader) getOffset(input *ObjectDataInput, variableOffsetsPos int32, index int32) int32 {
	offset := input.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return NULL_OFFSET
	}
	return int32(offset)
}

type DefaultCompactReader struct {
	offsetReader            OffsetReader
	in                      *ObjectDataInput
	serializer              CompactStreamSerializer
	schema                  Schema
	dataStartPosition       int32
	variableOffsetsPosition int32
}

func (r DefaultCompactReader) ReadBoolean(fieldName string) bool {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindBoolean:
		booleanOffset := fd.offset
		bitOffset := fd.bitOffset
		getOffset := booleanOffset + r.dataStartPosition
		lastByte := r.in.ReadByteAtPosition(getOffset)
		return ((lastByte >> byte(bitOffset)) & 1) != 0
	case pserialization.FieldKindNullableBoolean:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadBool()
		}, "Boolean").(*bool)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt8(fieldName string) int8 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt8:
		return r.in.ReadSignedByteAtPosition(r.readFixedSizePosition(fd))
	case pserialization.FieldKindNullableInt8:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadSignedByte()
		}, "Int8").(*int8)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt16(fieldName string) int16 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pserialization.FieldKindInt16:
		return r.in.ReadInt16AtPosition(r.readFixedSizePosition(fd))
	case pserialization.FieldKindNullableInt16:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt16()
		}, "Int16").(*int16)
	default:
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	case pserialization.FieldKindNullableInt32:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt32()
		}, "Int32").(*int32)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadInt64(fieldName string) int64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindInt64:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt64AtPosition(position)
	case pserialization.FieldKindNullableInt64:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadInt64()
		}, "Int64").(*int64)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat32(fieldName string) float32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindFloat32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadFloat32AtPosition(position)
	case pserialization.FieldKindNullableFloat32:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat32()
		}, "Float32").(*float32)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadFloat64(fieldName string) float64 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindFloat64:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadFloat64AtPosition(position)
	case pserialization.FieldKindNullableFloat64:
		return *r.getVariableSizeAsNonNull(fd, func(inp *ObjectDataInput) interface{} {
			return inp.ReadFloat64()
		}, "Float64").(*float64)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) *string {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindString)

	value := r.getVariableSize(fd, func(in *ObjectDataInput) interface{} {
		return in.ReadString()
	})

	if value == nil {
		return nil
	} else {
		return value.(*string)
	}
}

func (r DefaultCompactReader) ReadDecimal(fieldName string) *types.Decimal {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindDecimal)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return ReadDecimal(inp)
	}).(*types.Decimal)
}

func (r DefaultCompactReader) ReadTime(fieldName string) *types.LocalTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTime)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return ReadTime(inp)
	}).(*types.LocalTime)
}

func (r DefaultCompactReader) ReadDate(fieldName string) *types.LocalDate {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindDate)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return ReadDate(inp)
	}).(*types.LocalDate)
}

func (r DefaultCompactReader) ReadTimestamp(fieldName string) *types.LocalDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTimestamp)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return ReadTimestamp(inp)
	}).(*types.LocalDateTime)
}

func (r DefaultCompactReader) ReadTimestampWithTimezone(fieldName string) *types.OffsetDateTime {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindTimestampWithTimezone)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return ReadTimestampWithTimezone(inp)
	}).(*types.OffsetDateTime)
}

func (r DefaultCompactReader) ReadCompact(fieldName string) interface{} {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindCompact)
	return r.getVariableSize(fd, func(inp *ObjectDataInput) interface{} {
		return r.serializer.Read(r.in)
	})
}

func (r DefaultCompactReader) ReadArrayOfBoolean(fieldName string) []bool {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfInt8(fieldName string) []int8 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfInt16(fieldName string) []int16 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfInt32(fieldName string) []int32 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfInt64(fieldName string) []int64 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfFloat32(fieldName string) []float32 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfFloat64(fieldName string) []float64 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfString(fieldName string) []*string {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfDecimal(fieldName string) []*types.Decimal {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfTime(fieldName string) []*types.LocalTime {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfDate(fieldName string) []*types.LocalDate {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfTimestamp(fieldName string) []*types.LocalDateTime {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfTimestampWithTimezone(fieldName string) []*types.OffsetDateTime {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfCompact(fieldName string) []interface{} {
	return nil
}

func (r DefaultCompactReader) ReadNullableBoolean(fieldName string) *bool {
	return nil
}

func (r DefaultCompactReader) ReadNullableInt8(fieldName string) *int8 {
	return nil
}

func (r DefaultCompactReader) ReadNullableInt16(fieldName string) *int16 {
	return nil
}

func (r DefaultCompactReader) ReadNullableInt32(fieldName string) *int32 {
	return nil
}

func (r DefaultCompactReader) ReadNullableInt64(fieldName string) *int64 {
	return nil
}

func (r DefaultCompactReader) ReadNullableFloat32(fieldName string) *float32 {
	return nil
}

func (r DefaultCompactReader) ReadNullableFloat64(fieldName string) *float64 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableBoolean(fieldName string) []*bool {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableInt8(fieldName string) []*int8 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableInt16(fieldName string) []*int16 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableInt32(fieldName string) []*int32 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableInt64(fieldName string) []*int64 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat32(fieldName string) []*float32 {
	return nil
}

func (r DefaultCompactReader) ReadArrayOfNullableFloat64(fieldName string) []*float64 {
	return nil
}

func (r DefaultCompactReader) GetFieldKind(fieldName string) pserialization.FieldKind {
	return pserialization.FieldKindInt32
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	numberOfVarSizeFields := schema.numberOfVarSizeFields

	var variableOffsetsPosition, dataStartPosition, finalPosition int32

	if numberOfVarSizeFields == 0 {
		dataStartPosition = input.position
		finalPosition = dataStartPosition + schema.fixedSizeFieldsLength
		variableOffsetsPosition = 0
	} else {
		dataLength := input.readInt32()
		dataStartPosition = input.position
		variableOffsetsPosition = dataStartPosition + dataLength
		finalPosition = variableOffsetsPosition + numberOfVarSizeFields
	}
	input.SetPosition(finalPosition)

	return DefaultCompactReader{
		schema:                  schema,
		in:                      input,
		serializer:              serializer,
		dataStartPosition:       dataStartPosition,
		offsetReader:            &ByteOffsetReader{},
		variableOffsetsPosition: variableOffsetsPosition,
	}
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

func (r *DefaultCompactReader) readFixedSizePosition(fd FieldDescriptor) int32 {
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
	return ihzerrors.NewSerializationError(fmt.Sprintf("Error while reading %s. nil value cannot be read via get%s methods. Use getNullable%s instead", fieldName, methodSuffix, methodSuffix), nil)
}

func (r *DefaultCompactReader) getVariableSize(fd FieldDescriptor, reader func(*ObjectDataInput) interface{}) interface{} {
	currentPos := r.in.position
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == NULL_OFFSET {
		return nil
	}
	r.in.SetPosition(position)
	obj := reader(r.in)
	return &obj
}

func (r *DefaultCompactReader) getVariableSizeAsNonNull(fd FieldDescriptor, reader func(*ObjectDataInput) interface{}, methodSuffix string) interface{} {
	value := r.getVariableSize(fd, reader)
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
