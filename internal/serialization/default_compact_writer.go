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
	"fmt"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type DefaultCompactWriter struct {
	out          *PositionalObjectDataOutput
	fieldOffsets []int32
	schema       *Schema
	serializer   CompactStreamSerializer
	dataStartPos int32
}

func NewDefaultCompactWriter(serializer CompactStreamSerializer, out *PositionalObjectDataOutput, schema *Schema) *DefaultCompactWriter {
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
	return &DefaultCompactWriter{
		serializer:   serializer,
		out:          out,
		schema:       schema,
		fieldOffsets: fieldOffsets,
		dataStartPos: dataStartPosition,
	}
}

func (r *DefaultCompactWriter) WriteBoolean(fieldName string, value bool) {
	fd := r.getFieldDescriptorChecked(fieldName, pubserialization.FieldKindBoolean)
	offsetInBytes := fd.offset
	offsetInBits := fd.bitOffset
	writeOffset := offsetInBytes + r.dataStartPos
	r.out.PWriteBoolBit(writeOffset, int32(offsetInBits), value)
}

func (r *DefaultCompactWriter) WriteInt8(fieldName string, value int8) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindInt8)
	r.out.PWriteByte(position, byte(value))
}

func (r *DefaultCompactWriter) WriteInt16(fieldName string, value int16) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindInt16)
	r.out.PWriteInt16(position, value)
}

func (r *DefaultCompactWriter) WriteInt32(fieldName string, value int32) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindInt32)
	r.out.PWriteInt32(position, value)
}

func (r *DefaultCompactWriter) WriteInt64(fieldName string, value int64) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindInt64)
	r.out.PWriteInt64(position, value)
}

func (r *DefaultCompactWriter) WriteFloat32(fieldName string, value float32) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindFloat32)
	r.out.PWriteFloat32(position, value)
}

func (r *DefaultCompactWriter) WriteFloat64(fieldName string, value float64) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindFloat64)
	r.out.PWriteFloat64(position, value)
}

func (r *DefaultCompactWriter) WriteString(fieldName string, value *string) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindString)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

func (r *DefaultCompactWriter) WriteDecimal(fieldName string, value *types.Decimal) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindDecimal)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindDecimal, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDecimal(out, *v.(*types.Decimal))
	})
}

func (r *DefaultCompactWriter) WriteTime(fieldName string, value *types.LocalTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindTime)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindTime, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTime(out, time.Time(*v.(*types.LocalTime)))
	})
}

func (r *DefaultCompactWriter) WriteDate(fieldName string, value *types.LocalDate) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindDate)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindDate, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDate(out, time.Time(*v.(*types.LocalDate)))
	})
}

func (r *DefaultCompactWriter) WriteTimestamp(fieldName string, value *types.LocalDateTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindTimestamp)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindTimestamp, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestamp(out, time.Time(*v.(*types.LocalDateTime)))
	})
}

func (r *DefaultCompactWriter) WriteTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindTimestampWithTimezone)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindTimestampWithTimezone, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestampWithTimezone(out, time.Time(*v.(*types.OffsetDateTime)))
	})
}

func (r *DefaultCompactWriter) WriteCompact(fieldName string, value interface{}) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindCompact)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindCompact, value, func(out *PositionalObjectDataOutput, v interface{}) {
		r.serializer.Write(out, reflect.ValueOf(v).Interface())
	})
}

func (r *DefaultCompactWriter) WriteArrayOfBoolean(fieldName string, value []bool) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfBoolean)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfBoolean, value, func(out *PositionalObjectDataOutput, v interface{}) {
		r.writeBooleanBits(out, v.([]bool))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfInt8(fieldName string, value []int8) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfInt8)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfInt8, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt8Array(v.([]int8))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfInt16(fieldName string, value []int16) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfInt16)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfInt16, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16Array(v.([]int16))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfInt32(fieldName string, value []int32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfInt32)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfInt32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32Array(v.([]int32))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfInt64(fieldName string, value []int64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfInt64)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfInt64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64Array(v.([]int64))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfFloat32(fieldName string, value []float32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfFloat32)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfFloat32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32Array(v.([]float32))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfFloat64(fieldName string, value []float64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfFloat64)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindArrayOfFloat64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64Array(v.([]float64))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfString(fieldName string, value []*string) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfString)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfDecimal(fieldName string, value []*types.Decimal) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfDecimal)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDecimal, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDecimal(out, *v.(*types.Decimal))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfTime(fieldName string, value []*types.LocalTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfTime)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTime, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTime(out, time.Time(*v.(*types.LocalTime)))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfDate(fieldName string, value []*types.LocalDate) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfDate)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfDate, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteDate(out, time.Time(*v.(*types.LocalDate)))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfTimestamp(fieldName string, value []*types.LocalDateTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfTimestamp)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestamp, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestamp(out, time.Time(*v.(*types.LocalDateTime)))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfTimestampWithTimezone)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfTimestampWithTimezone, value, func(out *PositionalObjectDataOutput, v interface{}) {
		WriteTimestampWithTimezone(out, time.Time(*v.(*types.OffsetDateTime)))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfCompact(fieldName string, value []interface{}) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfCompact)
		return
	}
	var t reflect.Type
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfCompact, value, func(out *PositionalObjectDataOutput, v interface{}) {
		if t == nil {
			t = reflect.TypeOf(v)
		} else if reflect.TypeOf(v) != t {
			panic(ihzerrors.NewSerializationError("method WriteArrayOfCompact expects all parameters to be of the same type", nil))
		}
		r.serializer.Write(out, reflect.ValueOf(v).Interface())
	})
}

func (r *DefaultCompactWriter) WriteNullableBoolean(fieldName string, value *bool) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableBoolean)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableBoolean, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteBool(*v.(*bool))
	})
}

func (r *DefaultCompactWriter) WriteNullableInt8(fieldName string, value *int8) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableInt8)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableInt8, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteSignedByte(*v.(*int8))
	})
}

func (r *DefaultCompactWriter) WriteNullableInt16(fieldName string, value *int16) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableInt16)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableInt16, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16(*v.(*int16))
	})
}

func (r *DefaultCompactWriter) WriteNullableInt32(fieldName string, value *int32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableInt32)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableInt32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32(*v.(*int32))
	})
}

func (r *DefaultCompactWriter) WriteNullableInt64(fieldName string, value *int64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableInt64)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableInt64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64(*v.(*int64))
	})
}

func (r *DefaultCompactWriter) WriteNullableFloat32(fieldName string, value *float32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableFloat32)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableFloat32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32(*v.(*float32))
	})
}

func (r *DefaultCompactWriter) WriteNullableFloat64(fieldName string, value *float64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindNullableFloat64)
		return
	}
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindNullableFloat64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64(*v.(*float64))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableBoolean)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableBoolean, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteBool(*v.(*bool))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableInt8)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableInt8, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteSignedByte(*v.(*int8))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableInt16)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableInt16, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt16(*v.(*int16))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableInt32)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableInt32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt32(*v.(*int32))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableInt64)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableInt64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteInt64(*v.(*int64))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableFloat32)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableFloat32, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat32(*v.(*float32))
	})
}

func (r *DefaultCompactWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {
	if value == nil {
		r.setPositionAsNull(fieldName, pubserialization.FieldKindArrayOfNullableFloat64)
		return
	}
	r.writeArrayOfVariableSize(fieldName, pubserialization.FieldKindArrayOfNullableFloat64, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteFloat64(*v.(*float64))
	})
}

// End ends the serialization of the compact objects by writing the offsets of the variable-size fields as well as the data length, if there are some variable-size fields.
func (r *DefaultCompactWriter) End() {
	if r.schema.numberOfVarSizeFields == 0 {
		return
	}
	position := r.out.position
	dataLength := position - r.dataStartPos
	r.writeOffsets(dataLength, r.fieldOffsets)
	r.out.PWriteInt32(r.dataStartPos-Int32SizeInBytes, dataLength)
}

func (r *DefaultCompactWriter) getFieldDescriptorChecked(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	fd, ok := r.schema.GetField(fieldName)
	if !ok {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("invalid field name: '%s' for %v", fieldName, r.schema), nil))
	}
	if fd.Kind != fieldKind {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("invalid field type: '%s' for %v", fieldName, r.schema), nil))
	}
	return fd
}

func (r *DefaultCompactWriter) getFixedSizeFieldPosition(fieldName string, fieldKind pubserialization.FieldKind) int32 {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	return fd.offset + r.dataStartPos
}

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind pubserialization.FieldKind) {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	position := r.out.position
	fieldPosition := position - r.dataStartPos
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind pubserialization.FieldKind) {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	index := fd.index
	r.fieldOffsets[index] = -1
}

func (r *DefaultCompactWriter) writeVariableSizeField(fieldName string, fieldKind pubserialization.FieldKind, value interface{}, writer func(*PositionalObjectDataOutput, interface{})) {
	r.setPosition(fieldName, fieldKind)
	writer(r.out, value)
}

func (r *DefaultCompactWriter) writeArrayOfVariableSize(fieldName string, fieldKind pubserialization.FieldKind, values interface{}, writer func(*PositionalObjectDataOutput, interface{})) {
	value := reflect.ValueOf(values)
	r.setPosition(fieldName, fieldKind)
	dataLengthOffset := r.out.position
	r.out.WriteZeroBytes(Int32SizeInBytes)
	itemCount := value.Len()
	r.out.WriteInt32(int32(itemCount))

	offset := r.out.position
	offsets := make([]int32, itemCount)
	for i := 0; i < itemCount; i++ {
		element := value.Index(i).Interface()
		if !check.Nil(element) {
			offsets[i] = r.out.position - offset
			writer(r.out, element)
		} else {
			offsets[i] = -1
		}
	}
	dataLength := r.out.position - offset
	r.out.PWriteInt32(dataLengthOffset, dataLength)
	r.writeOffsets(dataLength, offsets)
}

func (r *DefaultCompactWriter) writeOffsets(dataLength int32, fieldOffsets []int32) {
	if dataLength < byteOffsetReaderRange {
		for _, offset := range fieldOffsets {
			r.out.WriteByte(byte(offset))
		}
	} else if dataLength < shortOffsetReaderRange {
		for _, offset := range fieldOffsets {
			r.out.WriteInt16(int16(offset))
		}
	} else {
		for _, offset := range fieldOffsets {
			r.out.WriteInt32(offset)
		}
	}
}

func (r *DefaultCompactWriter) writeBooleanBits(out *PositionalObjectDataOutput, booleans []bool) {
	var length int
	if booleans == nil {
		length = nilArrayLength
	} else {
		length = len(booleans)
	}
	r.out.WriteInt32(int32(length))
	position := r.out.position
	if length <= 0 {
		return
	}
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
