// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package serialization

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input DataInput, classDefinition *ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadByte(fieldName string) (byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, BYTE)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.ReadByte(fieldName)
}

func (mpr *MorphingPortableReader) ReadBool(fieldName string) (bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return false, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, BOOLEAN)
	if err != nil {
		return false, err
	}
	return mpr.DefaultPortableReader.ReadBool(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16(fieldName string) (uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, CHAR)
	if err != nil {
		return 0, err
	}
	return mpr.DefaultPortableReader.ReadUInt16(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16(fieldName string) (int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case SHORT:
		return mpr.DefaultPortableReader.ReadInt16(fieldName)
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int16(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, SHORT)
	}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) (int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case INT:
		return mpr.DefaultPortableReader.ReadInt32(fieldName)
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int32(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int32(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int32(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, INT)
	}
}

func (mpr *MorphingPortableReader) ReadInt64(fieldName string) (int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case LONG:
		return mpr.DefaultPortableReader.ReadInt64(fieldName)
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return int64(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int64(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int64(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int64(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, LONG)
	}
}

func (mpr *MorphingPortableReader) ReadFloat32(fieldName string) (float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case FLOAT:
		return mpr.DefaultPortableReader.ReadFloat32(fieldName)
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float32(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float32(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float32(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float32(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, FLOAT)
	}
}

func (mpr *MorphingPortableReader) ReadFloat64(fieldName string) (float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case DOUBLE:
		return mpr.DefaultPortableReader.ReadFloat64(fieldName)
	case LONG:
		ret, err := mpr.DefaultPortableReader.ReadInt64(fieldName)
		return float64(ret), err
	case FLOAT:
		ret, err := mpr.DefaultPortableReader.ReadFloat32(fieldName)
		return float64(ret), err
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float64(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float64(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float64(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float64(ret), err
	default:
		return 0, mpr.createIncompatibleClassChangeError(fieldDef, DOUBLE)
	}
}

func (mpr *MorphingPortableReader) ReadUTF(fieldName string) (string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return "", nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, UTF)
	if err != nil {
		return "", err
	}
	return mpr.DefaultPortableReader.ReadUTF(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortable(fieldName string) (Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, PORTABLE)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortable(fieldName)
}

func (mpr *MorphingPortableReader) ReadByteArray(fieldName string) ([]byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, BYTE_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadByteArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadBoolArray(fieldName string) ([]bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, BOOLEAN_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadBoolArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16Array(fieldName string) ([]uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, CHAR_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16Array(fieldName string) ([]int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, SHORT_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt32Array(fieldName string) ([]int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, INT_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt64Array(fieldName string) ([]int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, LONG_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadInt64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat32Array(fieldName string) ([]float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, FLOAT_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat64Array(fieldName string) ([]float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, DOUBLE_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadFloat64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadUTFArray(fieldName string) ([]string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, UTF_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadUTFArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortableArray(fieldName string) ([]Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	err := mpr.validateTypeCompatibility(fieldDef, PORTABLE_ARRAY)
	if err != nil {
		return nil, err
	}
	return mpr.DefaultPortableReader.ReadPortableArray(fieldName)
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd *FieldDefinition, expectedType int32) error {
	return core.NewHazelcastSerializationError(fmt.Sprintf("incompatible to read %v from %v while reading field : %v", getTypeByConst(expectedType), getTypeByConst(fd.fieldType), fd.fieldName), nil)
}

func (mpr *MorphingPortableReader) validateTypeCompatibility(fd *FieldDefinition, expectedType int32) error {
	if fd.fieldType != expectedType {
		return mpr.createIncompatibleClassChangeError(fd, expectedType)
	}
	return nil
}
