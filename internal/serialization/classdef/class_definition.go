// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package classdef

import (
	. "github.com/hazelcast/hazelcast-go-client/serialization"
)

type ClassDefinitionImpl struct {
	factoryId int32
	classId   int32
	version   int32
	fields    map[string]FieldDefinition
}

func NewClassDefinitionImpl(factoryId int32, classId int32, version int32) *ClassDefinitionImpl {
	return &ClassDefinitionImpl{factoryId, classId, version, make(map[string]FieldDefinition)}
}

func (cd *ClassDefinitionImpl) FactoryId() int32 {
	return cd.factoryId
}

func (cd *ClassDefinitionImpl) ClassId() int32 {
	return cd.classId
}

func (cd *ClassDefinitionImpl) Version() int32 {
	return cd.version
}

func (cd *ClassDefinitionImpl) Field(name string) FieldDefinition {
	return cd.fields[name]
}

func (cd *ClassDefinitionImpl) FieldCount() int {
	return len(cd.fields)
}

func (cd *ClassDefinitionImpl) AddFieldDefinition(definition FieldDefinition) {
	cd.fields[definition.Name()] = definition
}

type FieldDefinitionImpl struct {
	index     int32
	fieldName string
	fieldType int32
	factoryId int32
	classId   int32
	version   int32
}

func NewFieldDefinitionImpl(index int32, fieldName string, fieldType int32, factoryId int32, classId int32, version int32) *FieldDefinitionImpl {
	return &FieldDefinitionImpl{index, fieldName, fieldType, factoryId, classId, version}
}

func (fd *FieldDefinitionImpl) Type() int32 {
	return fd.fieldType
}

func (fd *FieldDefinitionImpl) Name() string {
	return fd.fieldName
}

func (fd *FieldDefinitionImpl) Index() int32 {
	return fd.index
}

func (fd *FieldDefinitionImpl) ClassId() int32 {
	return fd.classId
}

func (fd *FieldDefinitionImpl) FactoryId() int32 {
	return fd.factoryId
}

func (fd *FieldDefinitionImpl) Version() int32 {
	return fd.version
}

const (
	PORTABLE       = 0
	BYTE           = 1
	BOOL           = 2
	UINT16         = 3
	INT16          = 4
	INT32          = 5
	INT64          = 6
	FLOAT32        = 7
	FLOAT64        = 8
	UTF            = 9
	PORTABLE_ARRAY = 10
	BYTE_ARRAY     = 11
	BOOL_ARRAY     = 12
	UINT16_ARRAY   = 13
	INT16_ARRAY    = 14
	INT32_ARRAY    = 15
	INT64_ARRAY    = 16
	FLOAT32_ARRAY  = 17
	FLOAT64_ARRAY  = 18
	UTF_ARRAY      = 19
)
