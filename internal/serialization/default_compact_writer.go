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
	return DefaultCompactWriter{
		serializer: serializer,
		out:        out,
		schema:     schema,
	}
}

func (r *DefaultCompactWriter) getFieldDescriptorChecked(fieldName string, fieldKind FieldKind) (FieldDescriptor, error) {
	fd := r.schema.GetField(fieldName)
	if fd == nil {
		return FieldDescriptor{}, ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field name: '%s' for %s", fieldName, r.schema.ToString()), nil)
	}
	if fd.fieldKind != fieldKind {
		return FieldDescriptor{}, ihzerrors.NewSerializationError(fmt.Sprintf("Invalid field type: '%s' for %s", fieldName, r.schema.ToString()), nil)
	}
	return *fd, nil
}

func (r *DefaultCompactWriter) getFixedSizeFieldPosition(fieldName string, fieldKind FieldKind) (int32, error) {
	fd, err := r.getFieldDescriptorChecked(fieldName, fieldKind)
	if err != nil {
		return 0, err
	}
	return fd.offset + r.dataStartPosition, nil
}

func (r *DefaultCompactWriter) setPosition(fieldName string, fieldKind FieldKind) error {
	fd, err := r.getFieldDescriptorChecked(fieldName, fieldKind)
	if err != nil {
		return err
	}
	position := r.out.Position()
	fieldPosition := position - r.dataStartPosition
	index := fd.index
	r.fieldOffsets[index] = fieldPosition
	return nil
}

func (r *DefaultCompactWriter) setPositionAsNull(fieldName string, fieldKind FieldKind) error {
	fd, err := r.getFieldDescriptorChecked(fieldName, fieldKind)
	if err != nil {
		return err
	}
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

func (r DefaultCompactWriter) WriteInt32(fieldName string, value int32) error {
	position, err := r.getFixedSizeFieldPosition(fieldName, FieldKindInt32)
	if err != nil {
		return err
	}
	r.out.PWriteInt32(position, value)
	return nil
}

func (r DefaultCompactWriter) WriteString(fieldName string, value string) error {
	r.writeVariableSizeField(fieldName, FieldKindString, value, func(out *PositionalObjectDataOutput, v interface{}) {
		out.WriteString(v.(string))
	})
	return nil
}

func (r DefaultCompactWriter) End() {
}
