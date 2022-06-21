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
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type DefaultCompactWriter struct {
	out               *PositionalObjectDataOutput
	serializer        CompactStreamSerializer
	fieldOffsets      []int32
	schema            Schema
	dataStartPos int32
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
		dataStartPos: dataStartPosition,
	}
}

func (r DefaultCompactWriter) WriteInt32(fieldName string, value int32) {
	position := r.getFixedSizeFieldPosition(fieldName, pubserialization.FieldKindInt32)
	r.out.PWriteInt32(position, value)
}

func (r DefaultCompactWriter) WriteString(fieldName string, value *string) {
	r.writeVariableSizeField(fieldName, pubserialization.FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(*v.(*string))
	})
}

// End ends the serialization of the compact objects by writing the offsets of the variable-size fields as well as the data length, if there are some variable-size fields.
func (r DefaultCompactWriter) End() {
	if r.schema.numberOfVarSizeFields == 0 {
		return
	}
	position := r.out.Position()
	dataLength := position - r.dataStartPos
	r.writeOffsets(dataLength, r.fieldOffsets)
	r.out.PWriteInt32(r.dataStartPos-Int32SizeInBytes, dataLength)
}

func (r *DefaultCompactWriter) getFieldDescriptorChecked(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field name: '%s' for %v", fieldName, r.schema), nil))
	}
	if fd.fieldKind != fieldKind {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field type: '%s' for %v", fieldName, r.schema), nil))
	}
	return *fd
}

func (r *DefaultCompactWriter) getFixedSizeFieldPosition(fieldName string, fieldKind pubserialization.FieldKind) int32 {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	return fd.offset + r.dataStartPos
}

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind pubserialization.FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	position := r.out.Position()
	fieldPosition := position - r.dataStartPos
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
	return nil
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind pubserialization.FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	index := fd.index
	r.fieldOffsets[index] = -1
	return nil
}

func (r *DefaultCompactWriter) writeVariableSizeField(fieldName string, fieldKind pubserialization.FieldKind, value interface{}, writer func(*PositionalObjectDataOutput, interface{})) error {
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
	// Right now we don't need other offset writers
	for _, offset := range fieldOffsets {
		r.out.WriteByte(byte(offset))
	}
}
