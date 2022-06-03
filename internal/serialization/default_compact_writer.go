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

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const NULL_ARRAY_LENGTH = -1

type DefaultCompactWriter struct {
	out               *PositionalObjectDataOutput
	serializer        CompactStreamSerializer
	fieldOffsets      []int32
	schema            Schema
	dataStartPosition int32
}

func NewDefaultCompactWriter(serializer CompactStreamSerializer, out *PositionalObjectDataOutput, schema Schema) DefaultCompactWriter {
	var fieldOffsets []int32
	var dataStartPosition int32
	if schema.numberOfVarSizeFields != 0 {
		fieldOffsets = make([]int32, schema.numberOfVarSizeFields)
		dataStartPosition = out.position + Int32SizeInBytes
		// Skip for length and primitives.
		out.WriteZeroBytes(int(schema.fixedSizeFieldsLength + Int32SizeInBytes))
	} else {
		dataStartPosition = out.position
		// Skip for primitives. No need to write data length, when there is no
		// variable-size fields.
		out.WriteZeroBytes(int(schema.fixedSizeFieldsLength))
	}

	return DefaultCompactWriter{
		serializer:        serializer,
		out:               out,
		schema:            schema,
		fieldOffsets:      fieldOffsets,
		dataStartPosition: dataStartPosition,
	}
}

func (r DefaultCompactWriter) WriteBoolean(fieldName string, value bool) {
	fd := r.getFieldDescriptorChecked(fieldName, pserialization.FieldKindBoolean)
	offsetInBytes := fd.offset
	offsetInBits := fd.bitOffset
	writeOffset := offsetInBytes + r.dataStartPosition
	r.out.PWriteBoolBit(writeOffset, int32(offsetInBits), value)
}

func (r DefaultCompactWriter) WriteInt8(fieldName string, value int8) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindInt8)
	r.out.PWriteByte(position, byte(value))
}

func (r DefaultCompactWriter) WriteInt16(fieldName string, value int16) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindInt16)
	r.out.PWriteInt16(position, value)
}

func (r DefaultCompactWriter) WriteInt32(fieldName string, value int32) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindInt32)
	r.out.PWriteInt32(position, value)
}

func (r DefaultCompactWriter) WriteInt64(fieldName string, value int64) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindInt64)
	r.out.PWriteInt64(position, value)
}

func (r DefaultCompactWriter) WriteFloat32(fieldName string, value float32) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindFloat32)
	r.out.PWriteFloat32(position, value)
}

func (r DefaultCompactWriter) WriteFloat64(fieldName string, value float64) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindFloat64)
	r.out.PWriteFloat64(position, value)
}

func (r DefaultCompactWriter) WriteString(fieldName string, value *string) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

func (r DefaultCompactWriter) WriteDecimal(fieldName string, value *types.Decimal) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindDecimal, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDecimal(out, *v.(*types.Decimal))
	})
}

func (r DefaultCompactWriter) WriteTime(fieldName string, value *types.LocalTime) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindTime, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTime(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteDate(fieldName string, value *types.LocalDate) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindDate, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDate(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteTimestamp(fieldName string, value *types.LocalDateTime) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindTimestamp, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestamp(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindTimestampWithTimezone, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestampWithTimezone(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteCompact(fieldName string, value interface{}) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindCompact, value, func(out *PositionalObjectDataOutput, v interface{}) {
		r.serializer.Write(out, *v.(*interface{}))
	})
}

func (r DefaultCompactWriter) WriteArrayOfBoolean(fieldName string, value []bool) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfBoolean, value, func(out *PositionalObjectDataOutput, v interface{}) {
		r.writeBooleanBits(out, v.([]bool))
	})
}

func (r DefaultCompactWriter) WriteArrayOfInt8(fieldName string, value []int8) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfInt8, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt8Array(v.([]int8))
	})
}

func (r DefaultCompactWriter) WriteArrayOfInt16(fieldName string, value []int16) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfInt16, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16Array(v.([]int16))
	})
}

func (r DefaultCompactWriter) WriteArrayOfInt32(fieldName string, value []int32) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfInt32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32Array(v.([]int32))
	})
}

func (r DefaultCompactWriter) WriteArrayOfInt64(fieldName string, value []int64) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfInt64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64Array(v.([]int64))
	})
}

func (r DefaultCompactWriter) WriteArrayOfFloat32(fieldName string, value []float32) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfFloat32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32Array(v.([]float32))
	})
}

func (r DefaultCompactWriter) WriteArrayOfFloat64(fieldName string, value []float64) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindArrayOfFloat64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64Array(v.([]float64))
	})
}

func (r DefaultCompactWriter) WriteArrayOfString(fieldName string, value []*string) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfString, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

func (r DefaultCompactWriter) WriteArrayOfDecimal(fieldName string, value []*types.Decimal) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfDecimal, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDecimal(out, *v.(*types.Decimal))
	})
}

func (r DefaultCompactWriter) WriteArrayOfTime(fieldName string, value []*types.LocalTime) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTime, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTime(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteArrayOfDate(fieldName string, value []*types.LocalDate) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfDate, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDate(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteArrayOfTimestamp(fieldName string, value []*types.LocalDateTime) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTimestamp, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestamp(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteArrayOfTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfTimestampWithTimezone, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestampWithTimezone(out, *v.(*time.Time))
	})
}

func (r DefaultCompactWriter) WriteArrayOfCompact(fieldName string, value []interface{}) {
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfCompact, value, func(out *PositionalObjectDataOutput, v interface{}) {
		r.serializer.Write(out, *v.(*interface{}))
	})
}

func (r DefaultCompactWriter) WriteNullableBoolean(fieldName string, value *bool) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableBoolean, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteBool(*v.(*bool))
	})
}

func (r DefaultCompactWriter) WriteNullableInt8(fieldName string, value *int8) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableInt8, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteSignedByte(*v.(*int8))
	})
}

func (r DefaultCompactWriter) WriteNullableInt16(fieldName string, value *int16) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableInt16, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16(*v.(*int16))
	})
}

func (r DefaultCompactWriter) WriteNullableInt32(fieldName string, value *int32) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableInt32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32(*v.(*int32))
	})
}

func (r DefaultCompactWriter) WriteNullableInt64(fieldName string, value *int64) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableInt64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64(*v.(*int64))
	})
}

func (r DefaultCompactWriter) WriteNullableFloat32(fieldName string, value *float32) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableFloat32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32(*v.(*float32))
	})
}

func (r DefaultCompactWriter) WriteNullableFloat64(fieldName string, value *float64) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindNullableFloat64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64(*v.(*float64))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableBoolean, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteBool(*v.(*bool))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableInt8, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteSignedByte(*v.(*int8))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableInt16, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16(*v.(*int16))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableInt32, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32(*v.(*int32))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableInt64, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64(*v.(*int64))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableFloat32, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32(*v.(*float32))
	})
}

func (r DefaultCompactWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {
	interfaceValue := make([]interface{}, len(value))
	for i, v := range value {
		interfaceValue[i] = v
	}
	r.writeArrayOfVariableSize(fieldName, pserialization.FieldKindArrayOfNullableFloat64, interfaceValue, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64(*v.(*float64))
	})
}

/**
 * Ends the serialization of the compact objects by writing
 * the offsets of the variable-size fields as well as the
 * data length, if there are some variable-size fields.
 */
func (r DefaultCompactWriter) End() {
	if r.schema.numberOfVarSizeFields == 0 {
		return
	}
	position := r.out.position
	dataLength := position - r.dataStartPosition
	r.writeOffsets(dataLength, r.fieldOffsets)
	//write dataLength
	r.out.PWriteInt32(r.dataStartPosition-Int32SizeInBytes, dataLength)
}

func (r *DefaultCompactWriter) getFieldDescriptorChecked(fieldName string, fieldKind pserialization.FieldKind) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field name: '%s' for %s", fieldName, r.schema.ToString()), nil))
	}
	if fd.fieldKind != fieldKind {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field type: '%s' for %s", fieldName, r.schema.ToString()), nil))
	}
	return *fd
}

func (r *DefaultCompactWriter) getFixedSizeFieldPosition(fieldName string, fieldKind pserialization.FieldKind) int32 {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	return fd.offset + r.dataStartPosition
}

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind pserialization.FieldKind) {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	position := r.out.position
	fieldPosition := position - r.dataStartPosition
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind pserialization.FieldKind) {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	index := fd.index
	r.fieldOffsets[index] = -1
}

func (r *DefaultCompactWriter) writeVariableSizeField(fieldName string, fieldKind pserialization.FieldKind, value interface{}, writer func(*PositionalObjectDataOutput, interface{})) {
	if check.Nil(value) {
		r.setPositionAsNull(fieldName, fieldKind)
	} else {
		r.setPosition(fieldName, fieldKind)
		writer(r.out, value)
	}
}

func (r *DefaultCompactWriter) writeArrayOfVariableSize(fieldName string, fieldKind pserialization.FieldKind, values []interface{}, writer func(*PositionalObjectDataOutput, interface{})) {
	if values == nil {
		r.setPositionAsNull(fieldName, fieldKind)
		return
	}

	r.setPosition(fieldName, fieldKind)
	dataLengthOffset := r.out.position
	r.out.WriteZeroBytes(Int32SizeInBytes)
	itemCount := len(values)
	r.out.WriteInt32(int32(itemCount))

	offset := r.out.position
	offsets := make([]int32, itemCount)
	for i := 0; i < itemCount; i++ {
		if values[i] != nil {
			offsets[i] = r.out.position - offset
			writer(r.out, values[i])
		} else {
			offsets[i] = -1
		}
	}
	dataLength := r.out.position - offset
	r.out.PWriteInt32(dataLengthOffset, dataLength)
	r.writeOffsets(dataLength, offsets)
}

func (r DefaultCompactWriter) writeOffsets(dataLength int32, fieldOffsets []int32) {
	// Write now we don't need other offset writers
	for _, offset := range fieldOffsets {
		r.out.WriteByte(byte(offset))
	}
}

func (r DefaultCompactWriter) writeBooleanBits(out *PositionalObjectDataOutput, booleans []bool) {
	var length int
	if booleans == nil {
		length = NULL_ARRAY_LENGTH
	} else {
		length = len(booleans)
	}
	r.out.WriteInt32(int32(length))
	position := r.out.position
	if length > 0 {
		index := int32(0)
		r.out.WriteZeroBytes(1)
		for _, bool := range booleans {
			if index == BitsInAByte {
				index = 0
				r.out.WriteZeroBytes(1)
				position += 1
			}
			r.out.PWriteBoolBit(position, index, bool)
			index += 1
		}
	}
}
