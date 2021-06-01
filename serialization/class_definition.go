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

import "github.com/hazelcast/hazelcast-go-client/hzerrors"

// ClassDefinition defines a class schema for Portable structs.
type ClassDefinition struct {
	Fields    map[string]FieldDefinition
	FactoryID int32
	ClassID   int32
	Version   int32
}

func NewClassDefinition(factoryID int32, classID int32, version int32) *ClassDefinition {
	return &ClassDefinition{
		FactoryID: factoryID,
		ClassID:   classID,
		Version:   version,
		Fields:    make(map[string]FieldDefinition),
	}
}

func (cd *ClassDefinition) AddField(definition FieldDefinition) {
	cd.Fields[definition.Name] = definition
}

func (cd *ClassDefinition) AddByteField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeByte)
}

func (cd *ClassDefinition) AddBoolField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeBool)
}

func (cd *ClassDefinition) AddUInt16Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeUint16)
}

func (cd *ClassDefinition) AddInt16Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt16)
}

func (cd *ClassDefinition) AddInt32Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt32)
}

func (cd *ClassDefinition) AddInt64Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt64)
}

func (cd *ClassDefinition) AddFloat32Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeFloat32)
}

func (cd *ClassDefinition) AddFloat64Field(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeFloat64)
}

func (cd *ClassDefinition) AddStringField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeString)
}

func (cd *ClassDefinition) AddPortableField(fieldName string, def *ClassDefinition) error {
	if def.ClassID == 0 {
		return hzerrors.NewHazelcastIllegalArgumentError("Portable class ID cannot be zero", nil)
	}
	cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, TypePortable, def.FactoryID, def.ClassID, cd.Version))
	return nil
}

func (cd *ClassDefinition) addNewFieldDefinition(fieldName string, fieldType int32) {
	cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, fieldType, 0, 0, cd.Version))
}

func (cd *ClassDefinition) AddByteArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeByteArray)
}

func (cd *ClassDefinition) AddBoolArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeBoolArray)
}

func (cd *ClassDefinition) AddInt16ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt16Array)
}

func (cd *ClassDefinition) AddUInt16ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeUInt16Array)
}

func (cd *ClassDefinition) AddInt32ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt32Array)
}

func (cd *ClassDefinition) AddInt64ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeInt64Array)
}

func (cd *ClassDefinition) AddFloat32ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeFloat32Array)
}

func (cd *ClassDefinition) AddFloat64ArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeFloat64Array)
}

func (cd *ClassDefinition) AddPortableArrayField(fieldName string, def *ClassDefinition) error {
	if def.ClassID == 0 {
		return hzerrors.NewHazelcastIllegalArgumentError("Portable class ID cannot be zero", nil)
	}
	cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, TypePortableArray, def.FactoryID, def.ClassID, cd.Version))
	return nil
}

func (cd *ClassDefinition) AddStringArrayField(fieldName string) {
	cd.addNewFieldDefinition(fieldName, TypeStringArray)
}

const (
	TypePortable int32 = iota
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
	TypeUInt16Array
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

func newFieldDefinition(index int32, fieldName string, fieldType int32, factoryID int32,
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
