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
	"sort"
)

type Schema struct {
	fieldDefinitionMap map[string]FieldDescriptor
	typeName           string
	// Go does not have TreeMap, so we use a slice to store sorted fields
	fieldDefinitions      []FieldDescriptor
	id                    int64
	numberOfVarSizeFields int32
	fixedSizeFieldsLength int32
}

func NewSchema(typeName string, fieldDefinitionMap map[string]FieldDescriptor, rabin RabinFingerPrint) Schema {
	fds := make([]FieldDescriptor, len(fieldDefinitionMap))
	c := 0
	for _, fd := range fieldDefinitionMap {
		fds[c] = fd
		c += 1
	}
	// Sort according to field name
	sort.SliceStable(fds, func(i, j int) bool {
		return fds[i].fieldName < fds[j].fieldName
	})
	schema := Schema{
		typeName:           typeName,
		fieldDefinitionMap: fieldDefinitionMap,
		fieldDefinitions:   fds,
	}
	schema.init(rabin)
	return schema
}

func (s *Schema) GetField(fieldName string) (FieldDescriptor, bool) {
	fd, ok := s.fieldDefinitionMap[fieldName]
	return fd, ok
}

func (s *Schema) ID() int64 {
	return s.id
}

func (s *Schema) FieldCount() int {
	return len(s.fieldDefinitions)
}

func (s Schema) String() string {
	return fmt.Sprintf("Schema{typeName=%s, numberOfComplexFields=%d, primitivesLength=%d, fieldDefinitionMap=%v}",
		s.typeName, s.numberOfVarSizeFields, s.fixedSizeFieldsLength, s.fieldDefinitionMap)
}

func (s *Schema) TypeName() string {
	return s.typeName
}

func (s *Schema) init(rabin RabinFingerPrint) {
	fixedSizeFields := make([]FieldDescriptor, 0)
	variableSizeFields := make([]FieldDescriptor, 0)
	for _, descriptor := range s.fieldDefinitionMap {
		fieldKind := descriptor.fieldKind
		if FieldOperations(fieldKind).KindSizeInBytes() == variableKindSize {
			variableSizeFields = append(variableSizeFields, descriptor)
		} else {
			fixedSizeFields = append(fixedSizeFields, descriptor)
		}
	}
	sort.SliceStable(fixedSizeFields, func(i, j int) bool {
		kindSize1 := FieldOperations(fixedSizeFields[j].fieldKind).KindSizeInBytes()
		kindSize2 := FieldOperations(fixedSizeFields[i].fieldKind).KindSizeInBytes()
		return kindSize1 < kindSize2
	})
	var offset int32
	for _, descriptor := range fixedSizeFields {
		descriptor.offset = offset
		offset += FieldOperations(descriptor.fieldKind).KindSizeInBytes()
	}
	s.fixedSizeFieldsLength = offset
	for i, descriptor := range variableSizeFields {
		descriptor.index = int32(i)
	}
	s.numberOfVarSizeFields = int32(len(variableSizeFields))
	s.id = rabin.OfSchema(s)
}
