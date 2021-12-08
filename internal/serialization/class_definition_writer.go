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
	"time"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type ClassDefinitionWriter struct {
	portableContext *PortableContext
	classDefinition *serialization.ClassDefinition
}

func NewClassDefinitionWriter(portableContext *PortableContext, factoryID int32, classID int32,
	version int32) *ClassDefinitionWriter {
	return &ClassDefinitionWriter{portableContext,
		serialization.NewClassDefinition(factoryID, classID, version)}
}

func (cdw *ClassDefinitionWriter) WriteByte(fieldName string, value byte) {
	must(cdw.classDefinition.AddByteField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteBool(fieldName string, value bool) {
	must(cdw.classDefinition.AddBoolField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteUInt16(fieldName string, value uint16) {
	must(cdw.classDefinition.AddUInt16Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt16(fieldName string, value int16) {
	must(cdw.classDefinition.AddInt16Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt32(fieldName string, value int32) {
	must(cdw.classDefinition.AddInt32Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt64(fieldName string, value int64) {
	must(cdw.classDefinition.AddInt64Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteFloat32(fieldName string, value float32) {
	must(cdw.classDefinition.AddFloat32Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteFloat64(fieldName string, value float64) {
	must(cdw.classDefinition.AddFloat64Field(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteString(fieldName string, value string) {
	must(cdw.classDefinition.AddStringField(fieldName))
}

func (cdw *ClassDefinitionWriter) WritePortable(fieldName string, portable serialization.Portable) {
	if portable == nil {
		panic(ihzerrors.NewSerializationError("cannot write nil portable without explicitly registering class definition", nil))
	}
	nestedCD, err := cdw.portableContext.LookUpOrRegisterClassDefiniton(portable)
	if err != nil {
		panic(ihzerrors.NewSerializationError("looking up class definition", err))
	}
	must(cdw.classDefinition.AddPortableField(fieldName, nestedCD))
}

func (cdw *ClassDefinitionWriter) WriteNilPortable(fieldName string, factoryID int32, classID int32) {
	var version int32
	nestedCD := cdw.portableContext.LookUpClassDefinition(factoryID, classID, version)
	if nestedCD == nil {
		panic(ihzerrors.NewSerializationError("cannot write nil portable without explicitly registering class definition", nil))
	}
	must(cdw.classDefinition.AddPortableField(fieldName, nestedCD))
}

func (cdw *ClassDefinitionWriter) WriteByteArray(fieldName string, value []byte) {
	must(cdw.classDefinition.AddByteArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteBoolArray(fieldName string, value []bool) {
	must(cdw.classDefinition.AddBoolArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteUInt16Array(fieldName string, value []uint16) {
	must(cdw.classDefinition.AddUInt16ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt16Array(fieldName string, value []int16) {
	must(cdw.classDefinition.AddInt16ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt32Array(fieldName string, value []int32) {
	must(cdw.classDefinition.AddInt32ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteInt64Array(fieldName string, value []int64) {
	must(cdw.classDefinition.AddInt64ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteFloat32Array(fieldName string, value []float32) {
	must(cdw.classDefinition.AddFloat32ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteFloat64Array(fieldName string, value []float64) {
	must(cdw.classDefinition.AddFloat64ArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteStringArray(fieldName string, value []string) {
	must(cdw.classDefinition.AddStringArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteDate(fieldName string, value *time.Time) {
	must(cdw.classDefinition.AddDateField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTime(fieldName string, value *time.Time) {
	must(cdw.classDefinition.AddTimeField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTimestamp(fieldName string, value *time.Time) {
	must(cdw.classDefinition.AddTimestampField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTimestampWithTimezone(fieldName string, value *time.Time) {
	must(cdw.classDefinition.AddTimestampWithTimezoneField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteDateArray(fieldName string, value []time.Time) {
	must(cdw.classDefinition.AddDateArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTimeArray(fieldName string, value []time.Time) {
	must(cdw.classDefinition.AddTimeArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTimestampArray(fieldName string, value []time.Time) {
	must(cdw.classDefinition.AddTimestampArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WriteTimestampWithTimezoneArray(fieldName string, value []time.Time) {
	must(cdw.classDefinition.AddTimestampWithTimezoneArrayField(fieldName))
}

func (cdw *ClassDefinitionWriter) WritePortableArray(fieldName string, portables []serialization.Portable) {
	if len(portables) == 0 {
		panic(ihzerrors.NewSerializationError("cannot write empty array", nil))
	}
	var sample = portables[0]
	var nestedCD, err = cdw.portableContext.LookUpOrRegisterClassDefiniton(sample)
	if err != nil {
		panic(ihzerrors.NewSerializationError("looking up class definition", err))
	}
	if err = cdw.classDefinition.AddPortableArrayField(fieldName, nestedCD); err != nil {
		panic(ihzerrors.NewSerializationError("adding portable array field", err))
	}
}

func (cdw *ClassDefinitionWriter) GetRawDataOutput() serialization.DataOutput {
	return &EmptyObjectDataOutput{}
}

func (cdw *ClassDefinitionWriter) registerAndGet() (*serialization.ClassDefinition, error) {
	return cdw.classDefinition, cdw.portableContext.RegisterClassDefinition(cdw.classDefinition)
}

func must(err error) {
	if err != nil {
		panic(ihzerrors.NewSerializationError("", err))
	}
}
