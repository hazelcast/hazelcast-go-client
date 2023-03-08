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
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type SchemaWriter struct {
	fieldDefinitionMap map[string]FieldDescriptor
	typeName           string
}

func NewSchemaWriter(typeName string) SchemaWriter {
	return SchemaWriter{
		typeName:           typeName,
		fieldDefinitionMap: make(map[string]FieldDescriptor),
	}
}

func (s SchemaWriter) addField(fd FieldDescriptor) {
	s.fieldDefinitionMap[fd.Name] = fd
}

func (s SchemaWriter) Build() *Schema {
	return NewSchema(s.typeName, s.fieldDefinitionMap)
}

func (s SchemaWriter) WriteBoolean(fieldName string, value bool) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindBoolean))
}
func (s SchemaWriter) WriteInt8(fieldName string, value int8) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindInt8))
}
func (s SchemaWriter) WriteInt16(fieldName string, value int16) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindInt16))
}
func (s SchemaWriter) WriteInt32(fieldName string, value int32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindInt32))
}
func (s SchemaWriter) WriteInt64(fieldName string, value int64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindInt64))
}
func (s SchemaWriter) WriteFloat32(fieldName string, value float32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindFloat32))
}
func (s SchemaWriter) WriteFloat64(fieldName string, value float64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindFloat64))
}
func (s SchemaWriter) WriteString(fieldName string, value *string) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindString))
}
func (s SchemaWriter) WriteDecimal(fieldName string, value *types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindDecimal))
}
func (s SchemaWriter) WriteTime(fieldName string, value *types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindTime))
}
func (s SchemaWriter) WriteDate(fieldName string, value *types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindDate))
}
func (s SchemaWriter) WriteTimestamp(fieldName string, value *types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindTimestamp))
}
func (s SchemaWriter) WriteTimestampWithTimezone(fieldName string, value *types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindTimestampWithTimezone))
}
func (s SchemaWriter) WriteCompact(fieldName string, value interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindCompact))
}
func (s SchemaWriter) WriteArrayOfBoolean(fieldName string, value []bool) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfBoolean))
}
func (s SchemaWriter) WriteArrayOfInt8(fieldName string, value []int8) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfInt8))
}
func (s SchemaWriter) WriteArrayOfInt16(fieldName string, value []int16) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfInt16))
}
func (s SchemaWriter) WriteArrayOfInt32(fieldName string, value []int32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfInt32))
}
func (s SchemaWriter) WriteArrayOfInt64(fieldName string, value []int64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfInt64))
}
func (s SchemaWriter) WriteArrayOfFloat32(fieldName string, value []float32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfFloat32))
}
func (s SchemaWriter) WriteArrayOfFloat64(fieldName string, value []float64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfFloat64))
}
func (s SchemaWriter) WriteArrayOfString(fieldName string, value []*string) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfString))
}
func (s SchemaWriter) WriteArrayOfDecimal(fieldName string, value []*types.Decimal) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfDecimal))
}
func (s SchemaWriter) WriteArrayOfTime(fieldName string, value []*types.LocalTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfTime))
}
func (s SchemaWriter) WriteArrayOfDate(fieldName string, value []*types.LocalDate) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfDate))
}
func (s SchemaWriter) WriteArrayOfTimestamp(fieldName string, value []*types.LocalDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfTimestamp))
}
func (s SchemaWriter) WriteArrayOfTimestampWithTimezone(fieldName string, value []*types.OffsetDateTime) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfTimestampWithTimezone))
}
func (s SchemaWriter) WriteArrayOfCompact(fieldName string, value []interface{}) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfCompact))
}
func (s SchemaWriter) WriteNullableBoolean(fieldName string, value *bool) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableBoolean))
}
func (s SchemaWriter) WriteNullableInt8(fieldName string, value *int8) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableInt8))
}
func (s SchemaWriter) WriteNullableInt16(fieldName string, value *int16) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableInt16))
}
func (s SchemaWriter) WriteNullableInt32(fieldName string, value *int32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableInt32))
}
func (s SchemaWriter) WriteNullableInt64(fieldName string, value *int64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableInt64))
}
func (s SchemaWriter) WriteNullableFloat32(fieldName string, value *float32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableFloat32))
}
func (s SchemaWriter) WriteNullableFloat64(fieldName string, value *float64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindNullableFloat64))
}
func (s SchemaWriter) WriteArrayOfNullableBoolean(fieldName string, value []*bool) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableBoolean))
}
func (s SchemaWriter) WriteArrayOfNullableInt8(fieldName string, value []*int8) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableInt8))
}
func (s SchemaWriter) WriteArrayOfNullableInt16(fieldName string, value []*int16) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableInt16))
}
func (s SchemaWriter) WriteArrayOfNullableInt32(fieldName string, value []*int32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableInt32))
}
func (s SchemaWriter) WriteArrayOfNullableInt64(fieldName string, value []*int64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableInt64))
}
func (s SchemaWriter) WriteArrayOfNullableFloat32(fieldName string, value []*float32) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableFloat32))
}
func (s SchemaWriter) WriteArrayOfNullableFloat64(fieldName string, value []*float64) {
	s.addField(NewFieldDescriptor(fieldName, pubserialization.FieldKindArrayOfNullableFloat64))
}
