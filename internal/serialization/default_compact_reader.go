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

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

const NULL_OFFSET = -1

type OffsetReader interface {
	getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32
}

type ByteOffsetReader struct{}

func (ByteOffsetReader) getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32 {
	offset := input.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return NULL_OFFSET
	}
	return int32(offset)
}

type DefaultCompactReader struct {
	offsetReader            OffsetReader
	in                      *ObjectDataInput
	serializer              CompactStreamSerializer
	schema                  Schema
	dataStartPosition       int32
	variableOffsetsPosition int32
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case pserialization.FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) *string {
	fd := r.getFieldDefinitionChecked(fieldName, pserialization.FieldKindString)

	value := r.getVariableSize(fd, func(in *ObjectDataInput) interface{} {
		str := in.ReadString()
		return &str
	})

	if value == nil {
		return nil
	}
	return value.(*string)
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	numberOfVarSizeFields := schema.numberOfVarSizeFields

	var variableOffsetsPosition, dataStartPosition, finalPosition int32

	if numberOfVarSizeFields == 0 {
		dataStartPosition = input.Position()
		finalPosition = dataStartPosition + schema.fixedSizeFieldsLength
		variableOffsetsPosition = 0
	} else {
		dataLength := input.readInt32()
		dataStartPosition = input.Position()
		variableOffsetsPosition = dataStartPosition + dataLength
		finalPosition = variableOffsetsPosition + numberOfVarSizeFields
	}
	input.SetPosition(finalPosition)

	return DefaultCompactReader{
		schema:                  schema,
		in:                      input,
		serializer:              serializer,
		dataStartPosition:       dataStartPosition,
		offsetReader:            &ByteOffsetReader{},
		variableOffsetsPosition: variableOffsetsPosition,
	}
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(r.unknownField(fieldName))
	}
	return *fd
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind pserialization.FieldKind) FieldDescriptor {
	fd := r.getFieldDefinition(fieldName)
	if fd.fieldKind != fieldKind {
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
	return fd
}

func (r *DefaultCompactReader) readFixedSizePosition(fd FieldDescriptor) int32 {
	primitiveOffset := fd.offset
	return primitiveOffset + r.dataStartPosition
}

func (r *DefaultCompactReader) unknownField(fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unknown field name '%s' for %s", fieldName, r.schema.String()), nil)
}

func (r *DefaultCompactReader) unexpectedFieldKind(actualFieldKind pserialization.FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}

func (r *DefaultCompactReader) getVariableSize(fd FieldDescriptor, reader func(*ObjectDataInput) interface{}) interface{} {
	currentPos := r.in.Position()
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == NULL_OFFSET {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeFieldPosition(fd FieldDescriptor) int32 {
	index := fd.index
	offset := r.offsetReader.getOffset(r.in, r.variableOffsetsPosition, index)
	if offset == NULL_OFFSET {
		return NULL_OFFSET
	}
	return offset + r.dataStartPosition
}
