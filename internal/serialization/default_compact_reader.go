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
)

type DefaultCompactReader struct {
	schema            Schema
	in                *ObjectDataInput
	dataStartPosition int32
	serializer        CompactStreamSerializer
}

func (r DefaultCompactReader) ReadInt32(fieldName string) int32 {
	fd := r.getFieldDefinition(fieldName)
	fieldKind := fd.fieldKind
	switch fieldKind {
	case FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fieldName))
	}
}

func (r DefaultCompactReader) ReadString(fieldName string) string {
	fd := r.getFieldDefinitionChecked(fieldName, FieldKindString)

	value := r.getVariableSize(fd, func() interface{} {
		return r.in.ReadString()
	})
	return value.(string)
}

func NewDefaultCompactReader(serializer CompactStreamSerializer, input *ObjectDataInput, schema Schema) DefaultCompactReader {
	return DefaultCompactReader{
		schema:     schema,
		in:         input,
		serializer: serializer,
	}
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		panic(r.unknownField(fieldName))
	}
	return *fd
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind FieldKind) FieldDescriptor {
	fd := r.schema.GetField(fieldName)
	if fd.fieldKind != fieldKind {
		panic(r.unexpectedFieldKind(fd.fieldKind, fieldName))
	}
	return *fd
}

func (r *DefaultCompactReader) readFixedSizePosition(fd FieldDescriptor) int32 {
	primitiveOffset := fd.offset
	return primitiveOffset + r.dataStartPosition
}

func (r *DefaultCompactReader) unknownField(fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unknown field name '%s' for %s", fieldName, r.schema.ToString()), nil)
}

func (r *DefaultCompactReader) unexpectedFieldKind(actualFieldKind FieldKind, fieldName string) error {
	return ihzerrors.NewSerializationError(fmt.Sprintf("Unexpected field kind '%d' for field %s", actualFieldKind, fieldName), nil)
}

func (r *DefaultCompactReader) getVariableSize(fd FieldDescriptor, reader func() interface{}) interface{} {
	fieldKind := fd.fieldKind
	switch fieldKind {
	case FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position)
	default:
		panic(r.unexpectedFieldKind(fieldKind, fd.fieldName))
	}
}
