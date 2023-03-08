/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

type fieldDescriptorIndex struct {
	index int
	fd    FieldDescriptor
}

type Schema struct {
	Fields                map[string]FieldDescriptor
	TypeName              string
	fieldDefinitions      []FieldDescriptor
	id                    int64
	numberOfVarSizeFields int32
	fixedSizeFieldsLength int32
}

func NewSchema(typeName string, fieldDefinitionMap map[string]FieldDescriptor) *Schema {
	fds := make([]FieldDescriptor, 0, len(fieldDefinitionMap))
	for _, fd := range fieldDefinitionMap {
		fds = append(fds, fd)
	}
	// Sort according to field name
	sort.SliceStable(fds, func(i, j int) bool {
		return fds[i].Name < fds[j].Name
	})
	schema := &Schema{
		TypeName:         typeName,
		Fields:           make(map[string]FieldDescriptor, len(fieldDefinitionMap)),
		fieldDefinitions: fds,
	}
	schema.init()
	return schema
}

func (s *Schema) GetField(fieldName string) (FieldDescriptor, bool) {
	fd, ok := s.Fields[fieldName]
	return fd, ok
}

func (s *Schema) ID() int64 {
	return s.id
}

func (s *Schema) FieldCount() int {
	return len(s.fieldDefinitions)
}

func (s *Schema) FieldDefinitions() []FieldDescriptor {
	return s.fieldDefinitions
}

func (s Schema) String() string {
	return fmt.Sprintf("Schema{TypeName=%s, numberOfComplexFields=%d, primitivesLength=%d, Fields=%v}",
		s.TypeName, s.numberOfVarSizeFields, s.fixedSizeFieldsLength, s.Fields)
}

func (s *Schema) init() {
	var fixedSizeFields, varSizeFields, booleanFields []fieldDescriptorIndex
	// since FieldDescriptors are copied, it is mandatory to put them back to s.FieldDefinitions and s.Fields
	fdis := make([]fieldDescriptorIndex, len(s.fieldDefinitions))
	for i, fd := range s.fieldDefinitions {
		fdi := fieldDescriptorIndex{
			index: i,
			fd:    fd,
		}
		k := fd.Kind
		if FieldKindSize[k] == variableKindSize {
			varSizeFields = append(varSizeFields, fdi)
		} else {
			if k == serialization.FieldKindBoolean {
				booleanFields = append(booleanFields, fdi)
			} else {
				fixedSizeFields = append(fixedSizeFields, fdi)
			}
		}
		fdis[i] = fdi
	}
	sort.SliceStable(fixedSizeFields, func(i, j int) bool {
		s1 := FieldKindSize[fixedSizeFields[j].fd.Kind]
		s2 := FieldKindSize[fixedSizeFields[i].fd.Kind]
		return s1 < s2
	})
	var offset int32
	for _, fdi := range fixedSizeFields {
		fdi.fd.offset = offset
		offset += FieldKindSize[fdi.fd.Kind]
		s.fieldDefinitions[fdi.index] = fdi.fd
		s.Fields[fdi.fd.Name] = fdi.fd
	}
	bitOffset := 0
	for _, fdi := range booleanFields {
		fdi.fd.offset = offset
		fdi.fd.bitOffset = int8(bitOffset % BitsInAByte)
		bitOffset += 1
		if bitOffset%BitsInAByte == 0 {
			offset += 1
		}
		s.fieldDefinitions[fdi.index] = fdi.fd
		s.Fields[fdi.fd.Name] = fdi.fd
	}
	if bitOffset%BitsInAByte != 0 {
		offset += 1
	}
	s.fixedSizeFieldsLength = offset
	for i, fdi := range varSizeFields {
		fdi.fd.index = int32(i)
		s.fieldDefinitions[fdi.index] = fdi.fd
		s.Fields[fdi.fd.Name] = fdi.fd
	}
	s.numberOfVarSizeFields = int32(len(varSizeFields))
	s.id = RabinFingerPrint.OfSchema(s)
}
