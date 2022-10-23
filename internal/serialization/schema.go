/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"sort"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Schema struct {
	Fields                map[string]*FieldDescriptor
	TypeName              string
	fieldDefinitions      []*FieldDescriptor
	id                    int64
	numberOfVarSizeFields int32
	fixedSizeFieldsLength int32
}

func NewSchema(typeName string, fieldDefinitionMap map[string]*FieldDescriptor, rabin RabinFingerPrint) *Schema {
	fds := make([]*FieldDescriptor, len(fieldDefinitionMap))
	c := 0
	for _, fd := range fieldDefinitionMap {
		fds[c] = fd
		c += 1
	}
	// Sort according to field name
	sort.SliceStable(fds, func(i, j int) bool {
		return fds[i].FieldName < fds[j].FieldName
	})
	schema := &Schema{
		TypeName:         typeName,
		Fields:           fieldDefinitionMap,
		fieldDefinitions: fds,
	}
	schema.init(rabin)
	return schema
}

func (s *Schema) GetField(fieldName string) *FieldDescriptor {
	if fd, ok := s.Fields[fieldName]; ok {
		return fd
	}
	return nil
}

func (s *Schema) ID() int64 {
	return s.id
}

func (s *Schema) FieldCount() int {
	return len(s.fieldDefinitions)
}

func (s *Schema) FieldDefinitions() []*FieldDescriptor {
	return s.fieldDefinitions
}

func (s Schema) String() string {
	return fmt.Sprintf("Schema{TypeName=%s, numberOfComplexFields=%d, primitivesLength=%d, Fields=%v}",
		s.TypeName, s.numberOfVarSizeFields, s.fixedSizeFieldsLength, s.Fields)
}

func (s *Schema) init(rabin RabinFingerPrint) {
	var fixedSizeFields, varSizeFields, booleanFields []*FieldDescriptor

	for _, fd := range s.fieldDefinitions {
		fieldKind := fd.Kind
		if FieldOperations(fieldKind).KindSizeInBytes() == variableKindSize {
			varSizeFields = append(varSizeFields, fd)
		} else {
			if fieldKind == serialization.FieldKindBoolean {
				booleanFields = append(booleanFields, fd)
			} else {
				fixedSizeFields = append(fixedSizeFields, fd)
			}
		}
	}
	sort.SliceStable(fixedSizeFields, func(i, j int) bool {
		kindSize1 := FieldOperations(fixedSizeFields[j].Kind).KindSizeInBytes()
		kindSize2 := FieldOperations(fixedSizeFields[i].Kind).KindSizeInBytes()
		return kindSize1 < kindSize2
	})
	var offset int32
	for _, fd := range fixedSizeFields {
		fd.offset = offset
		offset += FieldOperations(fd.Kind).KindSizeInBytes()
	}

	bitOffset := 0
	for _, fd := range booleanFields {
		fd.offset = offset
		fd.bitOffset = int8(bitOffset % BitsInAByte)
		bitOffset += 1
		if bitOffset%BitsInAByte == 0 {
			offset += 1
		}
	}

	if bitOffset%BitsInAByte != 0 {
		offset += 1
	}

	s.fixedSizeFieldsLength = offset
	for i, fd := range varSizeFields {
		fd.index = int32(i)
	}
	s.numberOfVarSizeFields = int32(len(varSizeFields))
	s.id = rabin.OfSchema(s)
}
