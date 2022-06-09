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
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type SchemaWriter struct {
	fieldDefinitionMap map[string]*FieldDescriptor
	typeName           string
}

func NewSchemaWriter(typeName string) SchemaWriter {
	return SchemaWriter{
		typeName:           typeName,
		fieldDefinitionMap: make(map[string]*FieldDescriptor),
	}
}

func (s SchemaWriter) addField(fd FieldDescriptor) {
	s.fieldDefinitionMap[fd.fieldName] = &fd
}

func (s SchemaWriter) Build(rabin RabinFingerPrint) Schema {
	return NewSchema(s.typeName, s.fieldDefinitionMap, rabin)
}

func (s SchemaWriter) WriteBoolean(fieldName string, value bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindBoolean))
}
func (s SchemaWriter) WriteInt8(fieldName string, value int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt8))
}
func (s SchemaWriter) WriteInt16(fieldName string, value int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt16))
}
func (s SchemaWriter) WriteInt32(fieldName string, value int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt64(fieldName string, value int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt64))
}
func (s SchemaWriter) WriteFloat32(fieldName string, value float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindFloat32))
}
func (s SchemaWriter) WriteFloat64(fieldName string, value float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindFloat64))
}
func (s SchemaWriter) WriteString(fieldName string, value *string) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindString))
}
func (s SchemaWriter) WriteDecimal(fieldName string, value *types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindDecimal))
}
func (s SchemaWriter) WriteTime(fieldName string, value *types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindTime))
}
func (s SchemaWriter) WriteDate(fieldName string, value *types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindDate))
}
func (s SchemaWriter) WriteTimestamp(fieldName string, value *types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindTimestamp))
}
func (s SchemaWriter) WriteTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindTimestampWithTimezone))
}
func (s SchemaWriter) WriteCompact(fieldName string, value interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindCompact))
}
func (s SchemaWriter) WriteArrayOfBoolean(fieldName string, value []bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfBoolean))
}
func (s SchemaWriter) WriteArrayOfInt8(fieldName string, value []int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfInt8))
}
func (s SchemaWriter) WriteArrayOfInt16(fieldName string, value []int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfInt16))
}
func (s SchemaWriter) WriteArrayOfInt32(fieldName string, value []int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfInt32))
}
func (s SchemaWriter) WriteArrayOfInt64(fieldName string, value []int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfInt64))
}
func (s SchemaWriter) WriteArrayOfFloat32(fieldName string, value []float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfFloat32))
}
func (s SchemaWriter) WriteArrayOfFloat64(fieldName string, value []float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfFloat64))
}
func (s SchemaWriter) WriteArrayOfString(fieldName string, value []*string) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfString))
}
func (s SchemaWriter) WriteArrayOfDecimal(fieldName string, value []*types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfDecimal))
}
func (s SchemaWriter) WriteArrayOfTime(fieldName string, value []*types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfTime))
}
func (s SchemaWriter) WriteArrayOfDate(fieldName string, value []*types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfDate))
}
func (s SchemaWriter) WriteArrayOfTimestamp(fieldName string, value []*types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfTimestamp))
}
func (s SchemaWriter) WriteArrayOfTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfTimestampWithTimezone))
}
func (s SchemaWriter) WriteArrayOfCompact(fieldName string, value []interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfCompact))
}
func (s SchemaWriter) WriteNullableBoolean(fieldName string, value *bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableBoolean))
}
func (s SchemaWriter) WriteNullableInt8(fieldName string, value *int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableInt8))
}
func (s SchemaWriter) WriteNullableInt16(fieldName string, value *int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableInt16))
}
func (s SchemaWriter) WriteNullableInt32(fieldName string, value *int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableInt32))
}
func (s SchemaWriter) WriteNullableInt64(fieldName string, value *int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableInt64))
}
func (s SchemaWriter) WriteNullableFloat32(fieldName string, value *float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableFloat32))
}
func (s SchemaWriter) WriteNullableFloat64(fieldName string, value *float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindNullableFloat64))
}
func (s SchemaWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableBoolean))
}
func (s SchemaWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableInt8))
}
func (s SchemaWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableInt16))
}
func (s SchemaWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableInt64))
}
func (s SchemaWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableFloat32))
}
func (s SchemaWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindArrayOfNullableFloat64))
}
