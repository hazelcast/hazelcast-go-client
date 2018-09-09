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

package internal

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal/classdef"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input serialization.DataInput,
	classDefinition serialization.ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadByte(fieldName string) (byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeByte)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.ReadByte(fieldName)
}

func (mpr *MorphingPortableReader) ReadBool(fieldName string) (bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return false, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeBool)
	if err != nil {
		return false, err
	}
	return mpr.DefaultPortableReader.ReadBool(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16(fieldName string) (uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUint16)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.ReadUInt16(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16(fieldName string) (int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt16:
		return mpr.DefaultPortableReader.ReadInt16(fieldName)
	case classdef.TypeByte:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int16(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt16)
	}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) (int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt32:
		return mpr.DefaultPortableReader.ReadInt32(fieldName)
	case classdef.TypeByte:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int32(ret), err
	case classdef.TypeUint16:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int32(ret), err
	case classdef.TypeInt16:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int32(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt32)
	}
}

func (mpr *MorphingPortableReader) ReadInt64(fieldName string) (int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeInt64:
		return mpr.DefaultPortableReader.ReadInt64(fieldName)
	case classdef.TypeInt32:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return int64(ret), err
	case classdef.TypeByte:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int64(ret), err
	case classdef.TypeUint16:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int64(ret), err
	case classdef.TypeInt16:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int64(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeInt64)
	}
}

func (mpr *MorphingPortableReader) ReadFloat32(fieldName string) (float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeFloat32:
		return mpr.DefaultPortableReader.ReadFloat32(fieldName)
	case classdef.TypeInt32:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float32(ret), err
	case classdef.TypeByte:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float32(ret), err
	case classdef.TypeUint16:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float32(ret), err
	case classdef.TypeInt16:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float32(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeFloat32)
	}
}

func (mpr *MorphingPortableReader) ReadFloat64(fieldName string) (float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.Type() {
	case classdef.TypeFloat64:
		return mpr.DefaultPortableReader.ReadFloat64(fieldName)
	case classdef.TypeInt64:
		ret, err := mpr.DefaultPortableReader.ReadInt64(fieldName)
		return float64(ret), err
	case classdef.TypeFloat32:
		ret, err := mpr.DefaultPortableReader.ReadFloat32(fieldName)
		return float64(ret), err
	case classdef.TypeInt32:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float64(ret), err
	case classdef.TypeByte:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float64(ret), err
	case classdef.TypeUint16:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float64(ret), err
	case classdef.TypeInt16:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float64(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, classdef.TypeFloat64)
	}
}

func (mpr *MorphingPortableReader) ReadUTF(fieldName string) (string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return "", nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUTF)
	if err != nil {
		return "", err
	}
	return mpr.DefaultPortableReader.ReadUTF(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortable(fieldName string) (serialization.Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypePortable)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortable(fieldName)
}

func (mpr *MorphingPortableReader) ReadByteArray(fieldName string) ([]byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeByteArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadByteArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadBoolArray(fieldName string) ([]bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeBoolArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadBoolArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16Array(fieldName string) ([]uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUint16Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16Array(fieldName string) ([]int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt16Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt32Array(fieldName string) ([]int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt32Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt64Array(fieldName string) ([]int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeInt64Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat32Array(fieldName string) ([]float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeFloat32Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat64Array(fieldName string) ([]float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeFloat64Array)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadUTFArray(fieldName string) ([]string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypeUTFArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUTFArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortableArray(fieldName string) ([]serialization.Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.Field(fieldName)
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, classdef.TypePortableArray)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortableArray(fieldName)
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd serialization.FieldDefinition,
	expectedType int32) error {
	return core.NewHazelcastSerializationError(fmt.Sprintf("incompatible to read %v from %v while reading field : %v",
		TypeByID(expectedType), TypeByID(fd.Type()), fd.Name()), nil)
}

func (mpr *MorphingPortableReader) validateTypeCompatibility(fd serialization.FieldDefinition, expectedType int32) error {
	if fd.Type() != expectedType {
		return mpr.createIncompatibleClassChangeError(fd, expectedType)
	}
	return nil
}
