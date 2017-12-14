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
	. "github.com/hazelcast/go-client/core"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/serialization"
)

type DefaultPortableReader struct {
	serializer      *PortableSerializer
	input           DataInput
	classDefinition *ClassDefinition
	offset          int32
	finalPos        int32
	raw             bool
}

func NewDefaultPortableReader(serializer *PortableSerializer, input DataInput, classdefinition *ClassDefinition) *DefaultPortableReader {
	finalPos, _ := input.ReadInt32()
	input.ReadInt32()
	offset := input.Position()
	return &DefaultPortableReader{serializer, input, classdefinition, offset, finalPos, false}
}

func getTypeByConst(fieldType int32) string {
	var ret string
	switch t := fieldType; t {
	case PORTABLE:
		ret = "Portable"
	case BYTE:
		ret = "byte"
	case BOOLEAN:
		ret = "bool"
	case CHAR:
		ret = "uint16"
	case SHORT:
		ret = "int16"
	case INT:
		ret = "int32"
	case LONG:
		ret = "int64"
	case FLOAT:
		ret = "float32"
	case DOUBLE:
		ret = "float64"
	case UTF:
		ret = "string"
	case PORTABLE_ARRAY:
		ret = "[]Portable"
	case BYTE_ARRAY:
		ret = "[]byte"
	case BOOLEAN_ARRAY:
		ret = "[]bool"
	case CHAR_ARRAY:
		ret = "[]uint16"
	case SHORT_ARRAY:
		ret = "[]int16"
	case INT_ARRAY:
		ret = "[]int32"
	case LONG_ARRAY:
		ret = "[]int64"
	case FLOAT_ARRAY:
		ret = "[]float32"
	case DOUBLE_ARRAY:
		ret = "[]float64"
	case UTF_ARRAY:
		ret = "[]string"
	default:
	}
	return ret
}

func (pr *DefaultPortableReader) positionByField(fieldName string, fieldType int32) (int32, error) {
	field := pr.classDefinition.fields[fieldName]
	if pr.raw {
		return 0, NewHazelcastSerializationError("cannot read portable fields after getRawDataInput called", nil)

	}

	if field.fieldType != fieldType {
		return 0, NewHazelcastSerializationError(fmt.Sprintf("not a %s field: %s", getTypeByConst(fieldType), fieldName), nil)
	}
	pos, err := pr.input.(*ObjectDataInput).ReadInt32WithPosition(pr.offset + field.index*INT_SIZE_IN_BYTES)
	if err != nil {
		return 0, err
	}
	length, err := pr.input.(*ObjectDataInput).ReadInt16WithPosition(pos)
	if err != nil {
		return 0, err
	}
	return pos + SHORT_SIZE_IN_BYTES + int32(length) + 1, nil
}

func (pr *DefaultPortableReader) ReadByte(fieldName string) (byte, error) {
	pos, err := pr.positionByField(fieldName, BYTE)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadByteWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadBool(fieldName string) (bool, error) {
	pos, err := pr.positionByField(fieldName, BOOLEAN)
	if err != nil {
		return false, err
	}
	return pr.input.(*ObjectDataInput).ReadBoolWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadUInt16(fieldName string) (uint16, error) {
	pos, err := pr.positionByField(fieldName, CHAR)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadUInt16WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt16(fieldName string) (int16, error) {
	pos, err := pr.positionByField(fieldName, SHORT)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt16WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt32(fieldName string) (int32, error) {
	pos, err := pr.positionByField(fieldName, INT)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt32WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt64(fieldName string) (int64, error) {
	pos, err := pr.positionByField(fieldName, LONG)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt64WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat32(fieldName string) (float32, error) {
	pos, err := pr.positionByField(fieldName, FLOAT)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat32WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat64(fieldName string) (float64, error) {
	pos, err := pr.positionByField(fieldName, DOUBLE)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat64WithPosition(pos)
}

func (pr *DefaultPortableReader) ReadUTF(fieldName string) (string, error) {
	pos, err := pr.positionByField(fieldName, UTF)
	if err != nil {
		return "", err
	}
	return pr.input.(*ObjectDataInput).ReadUTFWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadPortable(fieldName string) (Portable, error) {
	backupPos := pr.input.Position()
	defer pr.input.SetPosition(backupPos)
	pos, err := pr.positionByField(fieldName, PORTABLE)
	if err != nil {
		return nil, err
	}
	pr.input.SetPosition(pos)
	isNil, err := pr.input.ReadBool()
	if err != nil {
		return nil, err
	}
	factoryId, err := pr.input.ReadInt32()
	if err != nil {
		return nil, err
	}
	classId, err := pr.input.ReadInt32()
	if err != nil {
		return nil, err
	}
	if isNil {
		return nil, nil
	}
	return pr.serializer.ReadObject(pr.input, factoryId, classId)
}

func (pr *DefaultPortableReader) ReadByteArray(fieldName string) ([]byte, error) {
	pos, err := pr.positionByField(fieldName, BYTE_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadByteArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadBoolArray(fieldName string) ([]bool, error) {
	pos, err := pr.positionByField(fieldName, BOOLEAN_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadBoolArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadUInt16Array(fieldName string) ([]uint16, error) {
	pos, err := pr.positionByField(fieldName, CHAR_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadUInt16ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt16Array(fieldName string) ([]int16, error) {
	pos, err := pr.positionByField(fieldName, SHORT_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt16ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt32Array(fieldName string) ([]int32, error) {
	pos, err := pr.positionByField(fieldName, INT_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt32ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt64Array(fieldName string) ([]int64, error) {
	pos, err := pr.positionByField(fieldName, LONG_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt64ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat32Array(fieldName string) ([]float32, error) {
	pos, err := pr.positionByField(fieldName, FLOAT_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat32ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat64Array(fieldName string) ([]float64, error) {
	pos, err := pr.positionByField(fieldName, DOUBLE_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat64ArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadUTFArray(fieldName string) ([]string, error) {
	pos, err := pr.positionByField(fieldName, UTF_ARRAY)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadUTFArrayWithPosition(pos)
}

func (pr *DefaultPortableReader) ReadPortableArray(fieldName string) ([]Portable, error) {
	backupPos := pr.input.Position()
	defer pr.input.SetPosition(backupPos)

	pos, err := pr.positionByField(fieldName, PORTABLE_ARRAY)
	if err != nil {
		return nil, err
	}
	pr.input.SetPosition(pos)
	length, err := pr.input.ReadInt32()
	if err != nil || length == NIL_ARRAY_LENGTH {
		return nil, err
	}
	factoryId, err := pr.input.ReadInt32()
	if err != nil {
		return nil, err
	}
	classId, err := pr.input.ReadInt32()
	if err != nil {
		return nil, err
	}
	var portables []Portable = make([]Portable, length)
	if length > 0 {
		offset := pr.input.Position()
		for i := int32(0); i < length; i++ {
			start, err := pr.input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*INT_SIZE_IN_BYTES)
			if err != nil {
				return nil, err
			}
			pr.input.SetPosition(start)
			portables[i], err = pr.serializer.ReadObject(pr.input, factoryId, classId)
			if err != nil {
				return nil, err
			}
		}
	}
	return portables, nil
}

func (pr *DefaultPortableReader) End() {
	pr.input.SetPosition(pr.finalPos)
}
