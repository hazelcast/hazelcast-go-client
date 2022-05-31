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
	schema Schema
	in *ObjectDataInput
	dataStartPosition int32
}

func (r *DefaultCompactReader) getFieldDefinition(fieldName string) (FieldDescriptor, error) {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		return FieldDescriptor{}, r.unknownField(fieldName)
	}
	return *fd, nil
}

func (r *DefaultCompactReader) getFieldDefinitionChecked(fieldName string, fieldKind FieldKind) (FieldDescriptor, error) {
	fd := r.schema.GetField(fieldName)
	if fd.fieldKind != fieldKind  {
		return FieldDescriptor{}, r.unexpectedFieldKind(fd.fieldKind, fieldName)
	}
	return *fd, nil
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


func (r *DefaultCompactReader) ReadInt32(fieldName string) (int32, error) {
	fd, err := r.getFieldDefinition(fieldName)
	if err != nil {
		return 0, err
	}
	fieldKind := fd.fieldKind
	switch fieldKind {
	case FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position), nil
	default:
		return 0, r.unexpectedFieldKind(fieldKind, fieldName)
	}
}


func (r *DefaultCompactReader) getVariableSize(fd FieldDescriptor, reader func () interface{}) (interface{}, error) {
	fieldKind := fd.fieldKind
	switch fieldKind {
	case FieldKindInt32:
		position := r.readFixedSizePosition(fd)
		return r.in.ReadInt32AtPosition(position), nil
	default:
		return 0, r.unexpectedFieldKind(fieldKind, fd.fieldName)
	}
}

func (r *DefaultCompactReader) ReadString(fieldName string) (string, error) {
	fd, err := r.getFieldDefinitionChecked(fieldName, FieldKindString)
	if err != nil {
		return "", err
	}

	value, err := r.getVariableSize(fd, func() interface{} {
		return r.in.ReadString()
	})
	return value.(string), err
}