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

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

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
		dataStartPosition = out.Position() + Int32SizeInBytes
		// Skip for length and primitives.
		out.WriteZeroBytes(int(schema.fixedSizeFieldsLength + Int32SizeInBytes))
	} else {
		dataStartPosition = out.Position()
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

}

func (r DefaultCompactWriter) WriteInt8(fieldName string, value int8) {

}

func (r DefaultCompactWriter) WriteInt16(fieldName string, value int16) {

}

func (r DefaultCompactWriter) WriteInt32(fieldName string, value int32) {
	position := r.getFixedSizeFieldPosition(fieldName, pserialization.FieldKindInt32)
	r.out.PWriteInt32(position, value)
}

func (r DefaultCompactWriter) WriteInt64(fieldName string, value int64) {

}

func (r DefaultCompactWriter) WriteFloat32(fieldName string, value float32) {

}

func (r DefaultCompactWriter) WriteFloat64(fieldName string, value float64) {

}

func (r DefaultCompactWriter) WriteNullableString(fieldName string, value *string) {
	r.writeVariableSizeField(fieldName, pserialization.FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

func (r DefaultCompactWriter) WriteNullableDecimal(fieldName string, value *types.Decimal) {

}

func (r DefaultCompactWriter) WriteNullableTime(fieldName string, value *types.LocalTime) {

}

func (r DefaultCompactWriter) WriteNullableDate(fieldName string, value *types.LocalDate) {

}

func (r DefaultCompactWriter) WriteNullableTimestamp(fieldName string, value *types.LocalDateTime) {

}

func (r DefaultCompactWriter) WriteNullableTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {

}

func (r DefaultCompactWriter) WriteNullableCompact(fieldName string, value interface{}) {

}

func (r DefaultCompactWriter) WriteArrayOfBoolean(fieldName string, value []bool) {

}

func (r DefaultCompactWriter) WriteArrayOfInt8(fieldName string, value []int8) {

}

func (r DefaultCompactWriter) WriteArrayOfInt16(fieldName string, value []int16) {

}

func (r DefaultCompactWriter) WriteArrayOfInt32(fieldName string, value []int32) {

}

func (r DefaultCompactWriter) WriteArrayOfInt64(fieldName string, value []int64) {

}

func (r DefaultCompactWriter) WriteArrayOfFloat32(fieldName string, value []float32) {

}

func (r DefaultCompactWriter) WriteArrayOfFloat64(fieldName string, value []float64) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableString(fieldName string, value []*string) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableDecimal(fieldName string, value []*types.Decimal) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableTime(fieldName string, value []*types.LocalTime) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableDate(fieldName string, value []*types.LocalDate) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableTimestamp(fieldName string, value []*types.LocalDateTime) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableCompact(fieldName string, value []interface{}) {

}

func (r DefaultCompactWriter) WriteNullableBoolean(fieldName string, value *bool) {

}

func (r DefaultCompactWriter) WriteNullableInt8(fieldName string, value *int8) {

}

func (r DefaultCompactWriter) WriteNullableInt16(fieldName string, value *int16) {

}

func (r DefaultCompactWriter) WriteNullableInt32(fieldName string, value *int32) {

}

func (r DefaultCompactWriter) WriteNullableInt64(fieldName string, value *int64) {

}

func (r DefaultCompactWriter) WriteNullableFloat32(fieldName string, value *float32) {

}

func (r DefaultCompactWriter) WriteNullableFloat64(fieldName string, value *float64) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {

}

func (r DefaultCompactWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {

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
	position := r.out.Position()
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

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind pserialization.FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	position := r.out.Position()
	fieldPosition := position - r.dataStartPosition
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
	return nil
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind pserialization.FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	index := fd.index
	r.fieldOffsets[index] = -1
	return nil
}

func (r *DefaultCompactWriter) writeVariableSizeField(fieldName string, fieldKind pserialization.FieldKind, value interface{}, writer func(*PositionalObjectDataOutput, interface{})) error {
	if check.Nil(value) {
		err := r.setPositionAsNull(fieldName, fieldKind)
		if err != nil {
			return err
		}
	} else {
		r.setPosition(fieldName, fieldKind)
		writer(r.out, value)
	}
	return nil
}

func (r DefaultCompactWriter) writeOffsets(dataLength int32, fieldOffsets []int32) {
	// Write now we don't need other offset writers
	for _, offset := range fieldOffsets {
		r.out.WriteByte(byte(offset))
	}
}
