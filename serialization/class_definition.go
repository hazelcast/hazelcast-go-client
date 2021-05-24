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

// ClassDefinition defines a class schema for Portable structs.
type ClassDefinition struct {
	fields    map[string]FieldDefinition
	factoryID int32
	classID   int32
	version   int32
}

func NewClassDefinition(factoryID int32, classID int32, version int32) *ClassDefinition {
	return &ClassDefinition{
		factoryID: factoryID,
		classID:   classID,
		version:   version,
		fields:    make(map[string]FieldDefinition),
	}
}

// FactoryID returns factory ID of struct.
func (cd *ClassDefinition) FactoryID() int32 {
	return cd.factoryID
}

// ClassID returns class ID of struct.
func (cd *ClassDefinition) ClassID() int32 {
	return cd.classID
}

// Version returns version of struct.
func (cd *ClassDefinition) Version() int32 {
	return cd.version
}

// Field returns field definition of field by given Name.
func (cd *ClassDefinition) Field(name string) (FieldDefinition, bool) {
	f, ok := cd.fields[name]
	return f, ok
}

// FieldCount returns the number of fields in struct.
func (cd *ClassDefinition) FieldCount() int {
	return len(cd.fields)
}

func (cd *ClassDefinition) AddFieldDefinition(definition FieldDefinition) {
	cd.fields[definition.Name] = definition
}

const (
	TypePortable = iota
	TypeByte
	TypeBool
	TypeUint16
	TypeInt16
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypePortableArray
	TypeByteArray
	TypeBoolArray
	TypeUint16Array
	TypeInt16Array
	TypeInt32Array
	TypeInt64Array
	TypeFloat32Array
	TypeFloat64Array
	TypeStringArray
)

// FieldDefinition defines name, type, index of a field.
type FieldDefinition struct {
	Name      string
	Index     int32
	Type      int32
	FactoryID int32
	ClassID   int32
	Version   int32
}

func NewFieldDefinition(index int32, fieldName string, fieldType int32, factoryID int32,
	classID int32, version int32) FieldDefinition {
	return FieldDefinition{
		Index:     index,
		Name:      fieldName,
		Type:      fieldType,
		FactoryID: factoryID,
		ClassID:   classID,
		Version:   version,
	}
}
