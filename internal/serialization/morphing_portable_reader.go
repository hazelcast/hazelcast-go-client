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

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input serialization.DataInput,
	classDefinition serialization.ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadByte(fieldName string) byte {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeByte); err != nil {
		panic(fmt.Errorf("MorphingPortableReader.ReadByte: %w", err))
	}
	return mpr.DefaultPortableReader.readByte(fieldName)
}

func (mpr *MorphingPortableReader) ReadBool(fieldName string) bool {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return false
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeBool); err != nil {
		panic(fmt.Errorf("MorphingPortableReader.ReadBool: %w", err))
	}
	return mpr.DefaultPortableReader.readBool(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16(fieldName string) uint16 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeUint16); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadUInt16(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16(fieldName string) int16 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	switch fieldDef.Type() {
	case TypeInt16:
		return mpr.DefaultPortableReader.ReadInt16(fieldName)
	case TypeByte:
		ret := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int16(ret)
	default:
		panic(mpr.createIncompatibleClassChangeError(fieldDef, TypeInt16))
	}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) int32 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	switch fieldDef.Type() {
	case TypeInt32:
		return mpr.DefaultPortableReader.ReadInt32(fieldName)
	case TypeByte:
		return int32(mpr.DefaultPortableReader.ReadByte(fieldName))
	case TypeUint16:
		return int32(mpr.DefaultPortableReader.ReadUInt16(fieldName))
	case TypeInt16:
		return int32(mpr.DefaultPortableReader.ReadInt16(fieldName))
	default:
		panic(mpr.createIncompatibleClassChangeError(fieldDef, TypeInt32))
	}
}

func (mpr *MorphingPortableReader) ReadInt64(fieldName string) int64 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	switch fieldDef.Type() {
	case TypeInt64:
		return mpr.DefaultPortableReader.ReadInt64(fieldName)
	case TypeInt32:
		return int64(mpr.DefaultPortableReader.ReadInt32(fieldName))
	case TypeByte:
		return int64(mpr.DefaultPortableReader.ReadByte(fieldName))
	case TypeUint16:
		return int64(mpr.DefaultPortableReader.ReadUInt16(fieldName))
	case TypeInt16:
		return int64(mpr.DefaultPortableReader.ReadInt16(fieldName))
	default:
		panic(mpr.createIncompatibleClassChangeError(fieldDef, TypeInt64))
	}
}

func (mpr *MorphingPortableReader) ReadFloat32(fieldName string) float32 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	switch fieldDef.Type() {
	case TypeFloat32:
		return mpr.DefaultPortableReader.ReadFloat32(fieldName)
	case TypeInt32:
		return float32(mpr.DefaultPortableReader.ReadInt32(fieldName))
	case TypeByte:
		return float32(mpr.DefaultPortableReader.ReadByte(fieldName))
	case TypeUint16:
		return float32(mpr.DefaultPortableReader.ReadUInt16(fieldName))
	case TypeInt16:
		return float32(mpr.DefaultPortableReader.ReadInt16(fieldName))
	default:
		panic(mpr.createIncompatibleClassChangeError(fieldDef, TypeFloat32))
	}
}

func (mpr *MorphingPortableReader) ReadFloat64(fieldName string) float64 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0
	}
	switch fieldDef.Type() {
	case TypeFloat64:
		return mpr.DefaultPortableReader.ReadFloat64(fieldName)
	case TypeInt64:
		return float64(mpr.DefaultPortableReader.ReadInt64(fieldName))
	case TypeFloat32:
		return float64(mpr.DefaultPortableReader.ReadFloat32(fieldName))
	case TypeInt32:
		return float64(mpr.DefaultPortableReader.ReadInt32(fieldName))
	case TypeByte:
		return float64(mpr.DefaultPortableReader.ReadByte(fieldName))
	case TypeUint16:
		return float64(mpr.DefaultPortableReader.ReadUInt16(fieldName))
	case TypeInt16:
		return float64(mpr.DefaultPortableReader.ReadInt16(fieldName))
	default:
		panic(mpr.createIncompatibleClassChangeError(fieldDef, TypeFloat64))
	}
}

func (mpr *MorphingPortableReader) ReadString(fieldName string) string {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return ""
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeUTF); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadString(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortable(fieldName string) serialization.Portable {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypePortable); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadPortable(fieldName)
}

func (mpr *MorphingPortableReader) ReadByteArray(fieldName string) []byte {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeByteArray); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadByteArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadBoolArray(fieldName string) []bool {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeBoolArray); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadBoolArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16Array(fieldName string) []uint16 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeUint16Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadUInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16Array(fieldName string) []int16 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeInt16Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt32Array(fieldName string) []int32 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeInt32Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadInt32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt64Array(fieldName string) []int64 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeInt64Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadInt64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat32Array(fieldName string) []float32 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeFloat32Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadFloat32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat64Array(fieldName string) []float64 {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeFloat64Array); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadFloat64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadStringArray(fieldName string) []string {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypeStringArray); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadStringArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortableArray(fieldName string) []serialization.Portable {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil
	}
	if err := mpr.validateTypeCompatibility(fieldDef, TypePortableArray); err != nil {
		panic(fmt.Errorf("error validating type compatibility: %w", err))
	}
	return mpr.DefaultPortableReader.ReadPortableArray(fieldName)
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd serialization.FieldDefinition,
	expectedType int32) error {
	return hzerror.NewHazelcastSerializationError(fmt.Sprintf("incompatible to read %v from %v while reading field : %v",
		TypeByID(expectedType), TypeByID(fd.Type()), fd.Name()), nil)
}

func (mpr *MorphingPortableReader) validateTypeCompatibility(fd serialization.FieldDefinition, expectedType int32) error {
	if fd.Type() != expectedType {
		return mpr.createIncompatibleClassChangeError(fd, expectedType)
	}
	return nil
}
