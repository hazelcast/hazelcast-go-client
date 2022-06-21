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
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

const nullOffset = -1

type OffsetReader interface {
	getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32
}

type ByteOffsetReader struct{}

func (ByteOffsetReader) getOffset(input *ObjectDataInput, variableOffsetsPos, index int32) int32 {
	offset := input.ReadByteAtPosition(variableOffsetsPos + index)
	if offset == 0xFF {
		return nullOffset
	}
	return int32(offset)
}

type DefaultCompactReader struct {
	offsetReader OffsetReader
	in           *ObjectDataInput
	serializer   CompactStreamSerializer
	schema       Schema
	startPos     int32
	offsetsPos   int32
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	switch fd.fieldKind {
	case pubserialization.FieldKindInt32:
		p := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(p)
	default:
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) *string {
	fd := r.getFieldDefinitionChecked(fieldName, pubserialization.FieldKindString)
	value := r.readVariableSizeField(fd, func(in *ObjectDataInput) interface{} {
		str := in.ReadString()
		return &str
	})
	if value == nil {
		return nil
	}
	return value.(*string)
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	var offsetsPos, startPos, finalPos int32
	if schema.numberOfVarSizeFields == 0 {
		startPos = input.Position()
		finalPos = startPos + schema.fixedSizeFieldsLength
	} else {
		dataLength := input.readInt32()
		startPos = input.Position()
		offsetsPos = startPos + dataLength
		finalPos = offsetsPos + schema.numberOfVarSizeFields
	}
	input.SetPosition(finalPos)
	return DefaultCompactReader{
		schema:       schema,
		in:           input,
		serializer:   serializer,
		startPos:     startPos,
		offsetReader: &ByteOffsetReader{},
		offsetsPos:   offsetsPos,
	}
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(newUnknownField(fieldName, r.schema))
	}
	return *fd
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind pubserialization.FieldKind) FieldDescriptor {
	fd := r.getFieldDefinition(fieldName)
	if fd.fieldKind != fieldKind {
		panic(newUnexpectedFieldKind(fd.fieldKind, fieldName))
	}
	return fd
}

func (r *DefaultCompactReader) readFixedSizePosition(fd FieldDescriptor) int32 {
	return fd.offset + r.startPos
}

func newUnknownField(fieldName string, schema Schema) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unknown field name '%s' for %s", fieldName, schema), nil)
}

func newUnexpectedFieldKind(actualFieldKind pubserialization.FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}

func (r *DefaultCompactReader) readVariableSizeField(fd FieldDescriptor, reader func(*ObjectDataInput) interface{}) interface{} {
	currentPos := r.in.Position()
	defer r.in.SetPosition(currentPos)
	position := r.readVariableSizeFieldPosition(fd)
	if position == nullOffset {
		return nil
	}
	r.in.SetPosition(position)
	return reader(r.in)
}

func (r *DefaultCompactReader) readVariableSizeFieldPosition(fd FieldDescriptor) int32 {
	offset := r.offsetReader.getOffset(r.in, r.offsetsPos, fd.index)
	if offset == nullOffset {
		return nullOffset
	}
	return offset + r.startPos
}
