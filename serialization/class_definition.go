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
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

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

func (cd *ClassDefinition) AddField(definition FieldDefinition) error {
	if _, ok := cd.Fields[definition.Name]; ok {
		return ihzerrors.NewIllegalArgumentError("duplicate field definition", nil)
	}
	cd.Fields[definition.Name] = definition
	return nil
}

func (cd *ClassDefinition) AddByteField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeByte)
}

func (cd *ClassDefinition) AddBoolField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeBool)
}

func (cd *ClassDefinition) AddUInt16Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeUint16)
}

func (cd *ClassDefinition) AddInt16Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt16)
}

func (cd *ClassDefinition) AddInt32Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt32)
}

func (cd *ClassDefinition) AddInt64Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt64)
}

func (cd *ClassDefinition) AddFloat32Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeFloat32)
}

func (cd *ClassDefinition) AddFloat64Field(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeFloat64)
}

func (cd *ClassDefinition) AddStringField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeString)
}

func (cd *ClassDefinition) AddPortableField(fieldName string, def *ClassDefinition) error {
	if def.ClassID == 0 {
		return ihzerrors.NewIllegalArgumentError("portable class ID cannot be zero", nil)
	}
	return cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, TypePortable, def.FactoryID, def.ClassID, cd.Version))
}

func (cd *ClassDefinition) addNewFieldDefinition(fieldName string, fieldType FieldDefinitionType) error {
	return cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, fieldType, 0, 0, cd.Version))
}

func (cd *ClassDefinition) AddByteArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeByteArray)
}

func (cd *ClassDefinition) AddBoolArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeBoolArray)
}

func (cd *ClassDefinition) AddInt16ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt16Array)
}

func (cd *ClassDefinition) AddUInt16ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeUInt16Array)
}

func (cd *ClassDefinition) AddInt32ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt32Array)
}

func (cd *ClassDefinition) AddInt64ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeInt64Array)
}

func (cd *ClassDefinition) AddFloat32ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeFloat32Array)
}

func (cd *ClassDefinition) AddFloat64ArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeFloat64Array)
}

func (cd *ClassDefinition) AddPortableArrayField(fieldName string, def *ClassDefinition) error {
	if def.ClassID == 0 {
		return ihzerrors.NewIllegalArgumentError("portable class ID cannot be zero", nil)
	}
	return cd.AddField(newFieldDefinition(int32(len(cd.Fields)), fieldName, TypePortableArray, def.FactoryID, def.ClassID, cd.Version))
}

func (cd *ClassDefinition) AddStringArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeStringArray)
}

func (cd *ClassDefinition) AddDateField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeDate)
}

func (cd *ClassDefinition) AddTimeField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTime)
}

func (cd *ClassDefinition) AddTimestampField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTimestamp)
}

func (cd *ClassDefinition) AddTimestampWithTimezoneField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTimestampWithTimezone)
}

func (cd *ClassDefinition) AddDateArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeDateArray)
}

func (cd *ClassDefinition) AddTimeArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTimeArray)
}

func (cd *ClassDefinition) AddTimestampArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTimestampArray)
}

func (cd *ClassDefinition) AddTimestampWithTimezoneArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeTimestampWithTimezoneArray)
}

func (cd *ClassDefinition) AddDecimalField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeDecimal)
}

func (cd *ClassDefinition) AddDecimalArrayField(fieldName string) error {
	return cd.addNewFieldDefinition(fieldName, TypeDecimalArray)
}

type FieldDefinitionType int32

const (
	TypePortable                   FieldDefinitionType = 0
	TypeByte                       FieldDefinitionType = 1
	TypeBool                       FieldDefinitionType = 2
	TypeUint16                     FieldDefinitionType = 3
	TypeInt16                      FieldDefinitionType = 4
	TypeInt32                      FieldDefinitionType = 5
	TypeInt64                      FieldDefinitionType = 6
	TypeFloat32                    FieldDefinitionType = 7
	TypeFloat64                    FieldDefinitionType = 8
	TypeString                     FieldDefinitionType = 9
	TypePortableArray              FieldDefinitionType = 10
	TypeByteArray                  FieldDefinitionType = 11
	TypeBoolArray                  FieldDefinitionType = 12
	TypeUInt16Array                FieldDefinitionType = 13
	TypeInt16Array                 FieldDefinitionType = 14
	TypeInt32Array                 FieldDefinitionType = 15
	TypeInt64Array                 FieldDefinitionType = 16
	TypeFloat32Array               FieldDefinitionType = 17
	TypeFloat64Array               FieldDefinitionType = 18
	TypeStringArray                FieldDefinitionType = 19
	TypeDecimal                    FieldDefinitionType = 20
	TypeDecimalArray               FieldDefinitionType = 21
	TypeTime                       FieldDefinitionType = 22
	TypeTimeArray                  FieldDefinitionType = 23
	TypeDate                       FieldDefinitionType = 24
	TypeDateArray                  FieldDefinitionType = 25
	TypeTimestamp                  FieldDefinitionType = 26
	TypeTimestampArray             FieldDefinitionType = 27
	TypeTimestampWithTimezone      FieldDefinitionType = 28
	TypeTimestampWithTimezoneArray FieldDefinitionType = 29
)

// FieldDefinition defines name, type, index of a field.
type FieldDefinition struct {
	Name      string
	Index     int32
	Type      FieldDefinitionType
	FactoryID int32
	ClassID   int32
	Version   int32
}

func newFieldDefinition(index int32, fieldName string, fieldType FieldDefinitionType, factoryID int32, classID int32, version int32) FieldDefinition {
	return FieldDefinition{
		Index:     index,
		Name:      fieldName,
		Type:      fieldType,
		FactoryID: factoryID,
		ClassID:   classID,
		Version:   version,
	}
}
