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
)

type DefaultCompactWriter struct {
	dataStartPosition int32
	schema            Schema
	out               *PositionalObjectDataOutput
	fieldOffsets      []int32
	serializer        CompactStreamSerializer
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

func (r *DefaultCompactWriter) getFieldDescriptorChecked(fieldName string, fieldKind FieldKind) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field name: '%s' for %s", fieldName, r.schema.ToString()), nil))
	}
	if fd.fieldKind != fieldKind {
		panic(ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field type: '%s' for %s", fieldName, r.schema.ToString()), nil))
	}
	return *fd
}

func (r *DefaultCompactWriter) getFixedSizeFieldPosition(fieldName string, fieldKind FieldKind) int32 {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	return fd.offset + r.dataStartPosition
}

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	position := r.out.Position()
	fieldPosition := position - r.dataStartPosition
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
	return nil
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind FieldKind) error {
	fd := r.getFieldDescriptorChecked(fieldName, fieldKind)
	index := fd.index
	r.fieldOffsets[index] = -1
	return nil
}

func (r *DefaultCompactWriter) writeVariableSizeField(fieldName string, fieldKind FieldKind, value interface{}, writer func(*PositionalObjectDataOutput, interface{})) error {
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

func (r DefaultCompactWriter) WriteInt32(fieldName string, value int32) {
	position := r.getFixedSizeFieldPosition(fieldName, FieldKindInt32)
	r.out.PWriteInt32(position, value)
}

func (r DefaultCompactWriter) WriteString(fieldName string, value string) {
	r.writeVariableSizeField(fieldName, FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(v.(string))
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
	position := r.out.Position()
	dataLength := position - r.dataStartPosition
	r.writeOffsets(dataLength, r.fieldOffsets)
	//write dataLength
	r.out.PWriteInt32(r.dataStartPosition-Int32SizeInBytes, dataLength)
}
