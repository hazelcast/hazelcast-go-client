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
	"unsafe"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type DefaultPortableWriter struct {
	serializer      *PortableSerializer
	output          *PositionalObjectDataOutput
	classDefinition *serialization.ClassDefinition
	begin           int32
	offset          int32
	raw             bool
}

func NewDefaultPortableWriter(serializer *PortableSerializer, output *PositionalObjectDataOutput,
	classDefinition *serialization.ClassDefinition) *DefaultPortableWriter {
	begin := output.Position()
	output.WriteZeroBytes(4)
	output.WriteInt32(int32(len(classDefinition.Fields)))
	offset := output.Position()
	fieldIndexesLength := (len(classDefinition.Fields) + 1) * Int32SizeInBytes
	output.WriteZeroBytes(fieldIndexesLength)
	return &DefaultPortableWriter{serializer, output, classDefinition, begin, offset, false}
}

func (pw *DefaultPortableWriter) WriteByte(fieldName string, value byte) {
	pw.setPosition(fieldName, int32(serialization.TypeByte))
	pw.output.WriteByte(value)
}

func (pw *DefaultPortableWriter) WriteBool(fieldName string, value bool) {
	pw.setPosition(fieldName, int32(serialization.TypeBool))
	pw.output.WriteBool(value)
}

func (pw *DefaultPortableWriter) WriteUInt16(fieldName string, value uint16) {
	pw.setPosition(fieldName, int32(serialization.TypeUint16))
	pw.output.WriteUInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt16(fieldName string, value int16) {
	pw.setPosition(fieldName, int32(serialization.TypeInt16))
	pw.output.WriteInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt32(fieldName string, value int32) {
	pw.setPosition(fieldName, int32(serialization.TypeInt32))
	pw.output.WriteInt32(value)
}

func (pw *DefaultPortableWriter) WriteInt64(fieldName string, value int64) {
	pw.setPosition(fieldName, int32(serialization.TypeInt64))
	pw.output.WriteInt64(value)
}

func (pw *DefaultPortableWriter) WriteFloat32(fieldName string, value float32) {
	pw.setPosition(fieldName, int32(serialization.TypeFloat32))
	pw.output.WriteFloat32(value)
}

func (pw *DefaultPortableWriter) WriteFloat64(fieldName string, value float64) {
	pw.setPosition(fieldName, int32(serialization.TypeFloat64))
	pw.output.WriteFloat64(value)
}

func (pw *DefaultPortableWriter) WriteString(fieldName string, value string) {
	pw.setPosition(fieldName, int32(serialization.TypeString))
	pw.output.WriteString(value)
}

func (pw *DefaultPortableWriter) WritePortable(fieldName string, portable serialization.Portable) {
	fieldDefinition, ok := pw.classDefinition.Fields[fieldName]
	if !ok {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("unknown field: %s", fieldName), nil))
	}
	pw.setPosition(fieldName, int32(serialization.TypePortable))
	isNullPortable := portable == nil
	pw.output.WriteBool(isNullPortable)
	pw.output.WriteInt32(fieldDefinition.FactoryID)
	pw.output.WriteInt32(fieldDefinition.ClassID)
	if !isNullPortable {
		pw.serializer.WriteObject(pw.output, portable)
	}
}

func (pw *DefaultPortableWriter) WriteNilPortable(fieldName string, factoryID int32, classID int32) {
	pw.setPosition(fieldName, int32(serialization.TypePortable))
	pw.output.WriteBool(true)
	pw.output.WriteInt32(factoryID)
	pw.output.WriteInt32(classID)
}

func (pw *DefaultPortableWriter) WriteByteArray(fieldName string, array []byte) {
	pw.setPosition(fieldName, int32(serialization.TypeByteArray))
	pw.output.WriteByteArray(array)
}

func (pw *DefaultPortableWriter) WriteBoolArray(fieldName string, array []bool) {
	pw.setPosition(fieldName, int32(serialization.TypeBoolArray))
	pw.output.WriteBoolArray(array)
}

func (pw *DefaultPortableWriter) WriteUInt16Array(fieldName string, array []uint16) {
	pw.setPosition(fieldName, int32(serialization.TypeUInt16Array))
	pw.output.WriteUInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt16Array(fieldName string, array []int16) {
	pw.setPosition(fieldName, int32(serialization.TypeInt16Array))
	pw.output.WriteInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt32Array(fieldName string, array []int32) {
	pw.setPosition(fieldName, int32(serialization.TypeInt32Array))
	pw.output.WriteInt32Array(array)
}

func (pw *DefaultPortableWriter) WriteInt64Array(fieldName string, array []int64) {
	pw.setPosition(fieldName, int32(serialization.TypeInt64Array))
	pw.output.WriteInt64Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat32Array(fieldName string, array []float32) {
	pw.setPosition(fieldName, int32(serialization.TypeFloat32Array))
	pw.output.WriteFloat32Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat64Array(fieldName string, array []float64) {
	pw.setPosition(fieldName, int32(serialization.TypeFloat64Array))
	pw.output.WriteFloat64Array(array)
}

func (pw *DefaultPortableWriter) WriteStringArray(fieldName string, array []string) {
	pw.setPosition(fieldName, int32(serialization.TypeStringArray))
	pw.output.WriteStringArray(array)
}

func (pw *DefaultPortableWriter) WritePortableArray(fieldName string, portableArray []serialization.Portable) {
	fieldDefinition, ok := pw.classDefinition.Fields[fieldName]
	if !ok {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("unknown field: %s", fieldName), nil))
	}
	pw.setPosition(fieldName, int32(serialization.TypePortableArray))
	length := len(portableArray)
	if portableArray == nil {
		length = nilArrayLength
	}
	pw.output.WriteInt32(int32(length))
	pw.output.WriteInt32(fieldDefinition.FactoryID)
	pw.output.WriteInt32(fieldDefinition.ClassID)
	if length <= 0 || portableArray == nil {
		// portableArray nil check is required just to avoid the warning about nil portableArray index
		return
	}
	innerOffset := pw.output.Position()
	pw.output.WriteZeroBytes(length * 4)
	for i := 0; i < length; i++ {
		sample := portableArray[i]
		posVal := pw.output.Position()
		pw.output.PWriteInt32(innerOffset+int32(i)*Int32SizeInBytes, posVal)
		pw.serializer.WriteObject(pw.output, sample)
	}
}

func (pw *DefaultPortableWriter) WriteDate(fieldName string, t *types.LocalDate) {
	pw.writeNullableField(fieldName, serialization.TypeDate, t == nil, func() {
		tt := (*time.Time)(t)
		WritePortableDate(pw.output.ObjectDataOutput, *tt)
	})
}

func (pw *DefaultPortableWriter) WriteTime(fieldName string, t *types.LocalTime) {
	pw.writeNullableField(fieldName, serialization.TypeTime, t == nil, func() {
		tt := (*time.Time)(t)
		WritePortableTime(pw.output.ObjectDataOutput, *tt)
	})
}

func (pw *DefaultPortableWriter) WriteTimestamp(fieldName string, t *types.LocalDateTime) {
	pw.writeNullableField(fieldName, serialization.TypeTimestamp, t == nil, func() {
		tt := (*time.Time)(t)
		WritePortableTimestamp(pw.output.ObjectDataOutput, *tt)
	})
}

func (pw *DefaultPortableWriter) WriteTimestampWithTimezone(fieldName string, t *types.OffsetDateTime) {
	pw.writeNullableField(fieldName, serialization.TypeTimestampWithTimezone, t == nil, func() {
		tt := (*time.Time)(t)
		WritePortableTimestampWithTimezone(pw.output.ObjectDataOutput, *tt)
	})
}

func (pw *DefaultPortableWriter) WriteDateArray(fieldName string, a []types.LocalDate) {
	pw.setPosition(fieldName, int32(serialization.TypeDateArray))
	ts := (*[]time.Time)(unsafe.Pointer(&a))
	pw.writeArrayOfTime(*ts, WritePortableDate)
}

func (pw *DefaultPortableWriter) WriteTimeArray(fieldName string, a []types.LocalTime) {
	pw.setPosition(fieldName, int32(serialization.TypeTimeArray))
	ts := (*[]time.Time)(unsafe.Pointer(&a))
	pw.writeArrayOfTime(*ts, WritePortableTime)
}

func (pw *DefaultPortableWriter) WriteTimestampArray(fieldName string, a []types.LocalDateTime) {
	pw.setPosition(fieldName, int32(serialization.TypeTimestampArray))
	ts := (*[]time.Time)(unsafe.Pointer(&a))
	pw.writeArrayOfTime(*ts, WritePortableTimestamp)
}

func (pw *DefaultPortableWriter) WriteTimestampWithTimezoneArray(fieldName string, a []types.OffsetDateTime) {
	pw.setPosition(fieldName, int32(serialization.TypeTimestampWithTimezoneArray))
	ts := (*[]time.Time)(unsafe.Pointer(&a))
	pw.writeArrayOfTime(*ts, WritePortableTimestampWithTimezone)
}

func (pw *DefaultPortableWriter) WriteDecimal(fieldName string, d *types.Decimal) {
	pw.writeNullableField(fieldName, serialization.TypeDecimal, d == nil, func() {
		WriteDecimal(pw.output.ObjectDataOutput, *d)
	})
}

func (pw *DefaultPortableWriter) WriteDecimalArray(fieldName string, ds []types.Decimal) {
	pw.setPosition(fieldName, int32(serialization.TypeDecimalArray))
	arrLen := len(ds)
	if arrLen == 0 {
		pw.output.WriteInt32(nilArrayLength)
		return
	}
	pw.output.WriteInt32(int32(arrLen))
	offset := pw.output.Position()
	pw.output.WriteZeroBytes(arrLen * Int32SizeInBytes)
	for i, v := range ds {
		pos := pw.output.Position()
		pw.output.PWriteInt32(offset+int32(Int32SizeInBytes*i), pos)
		WriteDecimal(pw.output.ObjectDataOutput, v)
	}
}

func (pw *DefaultPortableWriter) GetRawDataOutput() serialization.DataOutput {
	if !pw.raw {
		pos := pw.output.Position()
		index := int32(len(pw.classDefinition.Fields))
		pw.output.PWriteInt32(pw.offset+index*Int32SizeInBytes, pos)
		pw.raw = true
	}
	return pw.output.ObjectDataOutput
}

func (pw *DefaultPortableWriter) End() {
	position := pw.output.Position()
	pw.output.PWriteInt32(pw.begin, position)
}

func (pw *DefaultPortableWriter) setPosition(fieldName string, fieldType int32) {
	if pw.raw {
		panic(ihzerrors.NewSerializationError("cannot write Portable fields after getRawDataOutput() is called", nil))
	}
	field, ok := pw.classDefinition.Fields[fieldName]
	if !ok {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("unknown field: %s", fieldName), nil))
	}
	pos := pw.output.Position()
	pw.output.PWriteInt32(pw.offset+field.Index*Int32SizeInBytes, pos)
	runes := []rune(fieldName)
	pw.output.WriteInt16(int16(len(runes)))
	pw.output.writeStringBytes(runes)
	pw.output.WriteByte(byte(fieldType))
}

func (pw *DefaultPortableWriter) writeNullableField(fieldName string, fieldType serialization.FieldDefinitionType, isNil bool, f func()) {
	pw.setPosition(fieldName, int32(fieldType))
	pw.output.WriteBool(isNil)
	if !isNil {
		f()
	}
}

func (pw *DefaultPortableWriter) writeArrayOfTime(ts []time.Time, f func(o serialization.DataOutput, t time.Time)) {
	arrLen := len(ts)
	if arrLen == 0 {
		pw.output.WriteInt32(nilArrayLength)
		return
	}
	pw.output.WriteInt32(int32(arrLen))
	offset := pw.output.Position()
	pw.output.WriteZeroBytes(arrLen * Int32SizeInBytes)
	for i, t := range ts {
		pos := pw.output.Position()
		pw.output.PWriteInt32(offset+int32(Int32SizeInBytes*i), pos)
		f(pw.output.ObjectDataOutput, t)
	}
}

func WritePortableDate(o serialization.DataOutput, t time.Time) {
	y, m, d := t.Date()
	o.WriteInt16(int16(y))
	o.WriteByte(byte(m))
	o.WriteByte(byte(d))

}

func WritePortableTime(o serialization.DataOutput, t time.Time) {
	h, m, s := t.Clock()
	o.WriteByte(byte(h))
	o.WriteByte(byte(m))
	o.WriteByte(byte(s))
	o.WriteInt32(int32(t.Nanosecond()))
}

func WritePortableTimestamp(o serialization.DataOutput, t time.Time) {
	WritePortableDate(o, t)
	WritePortableTime(o, t)
}

func WritePortableTimestampWithTimezone(o serialization.DataOutput, t time.Time) {
	WritePortableTimestamp(o, t)
	_, off := t.Zone()
	o.WriteInt32(int32(off))
}
