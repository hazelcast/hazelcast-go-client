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
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt8(fieldName string, value int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt16(fieldName string, value int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt32(fieldName string, value int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt64(fieldName string, value int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteFloat32(fieldName string, value float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteFloat64(fieldName string, value float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableString(fieldName string, value *string) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindString))
}
func (s SchemaWriter) WriteNullableDecimal(fieldName string, value *types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableTime(fieldName string, value *types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableDate(fieldName string, value *types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableTimestamp(fieldName string, value *types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableCompact(fieldName string, value interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfBoolean(fieldName string, value []bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfInt8(fieldName string, value []int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfInt16(fieldName string, value []int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfInt32(fieldName string, value []int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfInt64(fieldName string, value []int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfFloat32(fieldName string, value []float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfFloat64(fieldName string, value []float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableString(fieldName string, value []*string) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableDecimal(fieldName string, value []*types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableTime(fieldName string, value []*types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableDate(fieldName string, value []*types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableTimestamp(fieldName string, value []*types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableCompact(fieldName string, value []interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableBoolean(fieldName string, value *bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableInt8(fieldName string, value *int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableInt16(fieldName string, value *int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableInt32(fieldName string, value *int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableInt64(fieldName string, value *int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableFloat32(fieldName string, value *float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteNullableFloat64(fieldName string, value *float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {
	s.addField(NewFieldDescriptor(fieldName, pserialization.FieldKindInt32))
}