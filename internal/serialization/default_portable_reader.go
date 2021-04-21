// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

type DefaultPortableReader struct {
	serializer      *PortableSerializer
	input           DataInput
	classDefinition ClassDefinition
	offset          int32
	finalPos        int32
	err             error
}

func NewDefaultPortableReader(serializer *PortableSerializer, input DataInput,
	classdefinition ClassDefinition) *DefaultPortableReader {
	finalPos := input.ReadInt32()
	input.ReadInt32()
	offset := input.Position()
	return &DefaultPortableReader{serializer, input,
		classdefinition, offset, finalPos, nil}
}

func (pr *DefaultPortableReader) Error() error {
	return pr.err
}

func TypeByID(fieldType int32) string {
	var ret string
	switch t := fieldType; t {
	case TypePortable:
		ret = "Portable"
	case TypeByte:
		ret = "byte"
	case TypeBool:
		ret = "bool"
	case TypeUint16:
		ret = "uint16"
	case TypeInt16:
		ret = "int16"
	case TypeInt32:
		ret = "int32"
	case TypeInt64:
		ret = "int64"
	case TypeFloat32:
		ret = "float32"
	case TypeFloat64:
		ret = "float64"
	case TypeUTF:
		ret = "string"
	case TypePortableArray:
		ret = "[]Portable"
	case TypeByteArray:
		ret = "[]byte"
	case TypeBoolArray:
		ret = "[]bool"
	case TypeUint16Array:
		ret = "[]uint16"
	case TypeInt16Array:
		ret = "[]int16"
	case TypeInt32Array:
		ret = "[]int32"
	case TypeInt64Array:
		ret = "[]int64"
	case TypeFloat32Array:
		ret = "[]float32"
	case TypeFloat64Array:
		ret = "[]float64"
	case TypeUTFArray:
		ret = "[]string"
	default:
	}
	return ret
}

func (pr *DefaultPortableReader) positionByField(fieldName string, fieldType int32) (int32, error) {
	field := pr.classDefinition.Field(fieldName)
	if field.Type() != fieldType {
		return 0, hzerror.NewHazelcastSerializationError(fmt.Sprintf("not a %s field: %s", TypeByID(fieldType), fieldName), nil)
	}
	pos := pr.input.(*ObjectDataInput).ReadInt32WithPosition(pr.offset + field.Index()*bufutil.Int32SizeInBytes)
	length := pr.input.(*ObjectDataInput).ReadInt16WithPosition(pos)
	return pos + bufutil.Int16SizeInBytes + int32(length) + 1, pr.input.Error()
}

func (pr *DefaultPortableReader) ReadByte(fieldName string) byte {
	if pr.err != nil {
		return 0
	}
	var res byte
	res, pr.err = pr.readByte(fieldName)
	return res
}

func (pr *DefaultPortableReader) readByte(fieldName string) (byte, error) {
	pos, err := pr.positionByField(fieldName, TypeByte)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadByteWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadBool(fieldName string) bool {
	if pr.err != nil {
		return false
	}
	var res bool
	res, pr.err = pr.readBool(fieldName)
	return res
}

func (pr *DefaultPortableReader) readBool(fieldName string) (bool, error) {
	pos, err := pr.positionByField(fieldName, TypeBool)
	if err != nil {
		return false, err
	}
	return pr.input.(*ObjectDataInput).ReadBoolWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadUInt16(fieldName string) uint16 {
	if pr.err != nil {
		return 0
	}
	var res uint16
	res, pr.err = pr.readUInt16(fieldName)
	return res
}

func (pr *DefaultPortableReader) readUInt16(fieldName string) (uint16, error) {
	pos, err := pr.positionByField(fieldName, TypeUint16)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadUInt16WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt16(fieldName string) int16 {
	if pr.err != nil {
		return 0
	}
	var res int16
	res, pr.err = pr.readInt16(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt16(fieldName string) (int16, error) {
	pos, err := pr.positionByField(fieldName, TypeInt16)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt16WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt32(fieldName string) int32 {
	if pr.err != nil {
		return 0
	}
	var res int32
	res, pr.err = pr.readInt32(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt32(fieldName string) (int32, error) {
	pos, err := pr.positionByField(fieldName, TypeInt32)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt32WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt64(fieldName string) int64 {
	if pr.err != nil {
		return 0
	}
	var res int64
	res, pr.err = pr.readInt64(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt64(fieldName string) (int64, error) {
	pos, err := pr.positionByField(fieldName, TypeInt64)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadInt64WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadFloat32(fieldName string) float32 {
	if pr.err != nil {
		return 0
	}
	var res float32
	res, pr.err = pr.readFloat32(fieldName)
	return res
}

func (pr *DefaultPortableReader) readFloat32(fieldName string) (float32, error) {
	pos, err := pr.positionByField(fieldName, TypeFloat32)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat32WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadFloat64(fieldName string) float64 {
	if pr.err != nil {
		return 0
	}
	var res float64
	res, pr.err = pr.readFloat64(fieldName)
	return res
}

func (pr *DefaultPortableReader) readFloat64(fieldName string) (float64, error) {
	pos, err := pr.positionByField(fieldName, TypeFloat64)
	if err != nil {
		return 0, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat64WithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadUTF(fieldName string) string {
	if pr.err != nil {
		return ""
	}
	var res string
	res, pr.err = pr.readUTF(fieldName)
	return res
}

func (pr *DefaultPortableReader) readUTF(fieldName string) (string, error) {
	pos, err := pr.positionByField(fieldName, TypeUTF)
	if err != nil {
		return "", err
	}
	return pr.input.(*ObjectDataInput).ReadUTFWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadPortable(fieldName string) Portable {
	if pr.err != nil {
		return nil
	}
	var res Portable
	res, pr.err = pr.readPortable(fieldName)
	return res
}

func (pr *DefaultPortableReader) readPortable(fieldName string) (Portable, error) {
	backupPos := pr.input.Position()
	defer pr.input.SetPosition(backupPos)
	pos, err := pr.positionByField(fieldName, TypePortable)
	if err != nil {
		return nil, err
	}
	pr.input.SetPosition(pos)
	isNil := pr.input.ReadBool()
	factoryID := pr.input.ReadInt32()
	classID := pr.input.ReadInt32()
	if pr.input.Error() != nil {
		return nil, pr.input.Error()
	}
	if isNil {
		return nil, nil
	}
	return pr.serializer.ReadObject(pr.input, factoryID, classID)
}

func (pr *DefaultPortableReader) ReadByteArray(fieldName string) []byte {
	if pr.err != nil {
		return nil
	}
	var res []byte
	res, pr.err = pr.readByteArray(fieldName)
	return res
}

func (pr *DefaultPortableReader) readByteArray(fieldName string) ([]byte, error) {
	pos, err := pr.positionByField(fieldName, TypeByteArray)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadByteArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadBoolArray(fieldName string) []bool {
	if pr.err != nil {
		return nil
	}
	var res []bool
	res, pr.err = pr.readBoolArray(fieldName)
	return res
}

func (pr *DefaultPortableReader) readBoolArray(fieldName string) ([]bool, error) {
	pos, err := pr.positionByField(fieldName, TypeBoolArray)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadBoolArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadUInt16Array(fieldName string) []uint16 {
	if pr.err != nil {
		return nil
	}
	var res []uint16
	res, pr.err = pr.readUInt16Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readUInt16Array(fieldName string) ([]uint16, error) {
	pos, err := pr.positionByField(fieldName, TypeUint16Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadUInt16ArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt16Array(fieldName string) []int16 {
	if pr.err != nil {
		return nil
	}
	var res []int16
	res, pr.err = pr.readInt16Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt16Array(fieldName string) ([]int16, error) {
	pos, err := pr.positionByField(fieldName, TypeInt16Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt16ArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt32Array(fieldName string) []int32 {
	if pr.err != nil {
		return nil
	}
	var res []int32
	res, pr.err = pr.readInt32Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt32Array(fieldName string) ([]int32, error) {
	pos, err := pr.positionByField(fieldName, TypeInt32Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt32ArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadInt64Array(fieldName string) []int64 {
	if pr.err != nil {
		return nil
	}
	var res []int64
	res, pr.err = pr.readInt64Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readInt64Array(fieldName string) ([]int64, error) {
	pos, err := pr.positionByField(fieldName, TypeInt64Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadInt64ArrayWithPosition(pos), pr.input.Error()
}
func (pr *DefaultPortableReader) ReadFloat32Array(fieldName string) []float32 {
	if pr.err != nil {
		return nil
	}
	var res []float32
	res, pr.err = pr.readFloat32Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readFloat32Array(fieldName string) ([]float32, error) {
	pos, err := pr.positionByField(fieldName, TypeFloat32Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat32ArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadFloat64Array(fieldName string) []float64 {
	if pr.err != nil {
		return nil
	}
	var res []float64
	res, pr.err = pr.readFloat64Array(fieldName)
	return res
}

func (pr *DefaultPortableReader) readFloat64Array(fieldName string) ([]float64, error) {
	pos, err := pr.positionByField(fieldName, TypeFloat64Array)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadFloat64ArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadUTFArray(fieldName string) []string {
	if pr.err != nil {
		return nil
	}
	var res []string
	res, pr.err = pr.readUTFArray(fieldName)
	return res
}

func (pr *DefaultPortableReader) readUTFArray(fieldName string) ([]string, error) {
	pos, err := pr.positionByField(fieldName, TypeUTFArray)
	if err != nil {
		return nil, err
	}
	return pr.input.(*ObjectDataInput).ReadUTFArrayWithPosition(pos), pr.input.Error()
}

func (pr *DefaultPortableReader) ReadPortableArray(fieldName string) []Portable {
	if pr.err != nil {
		return nil
	}
	var res []Portable
	res, pr.err = pr.readPortableArray(fieldName)
	return res
}

func (pr *DefaultPortableReader) readPortableArray(fieldName string) ([]Portable, error) {
	backupPos := pr.input.Position()
	defer pr.input.SetPosition(backupPos)

	pos, err := pr.positionByField(fieldName, TypePortableArray)
	if err != nil {
		return nil, err
	}
	pr.input.SetPosition(pos)
	length := pr.input.ReadInt32()
	factoryID := pr.input.ReadInt32()
	classID := pr.input.ReadInt32()
	if pr.input.Error() != nil || length == bufutil.NilArrayLength {
		return nil, pr.input.Error()
	}
	var portables = make([]Portable, length)
	if length > 0 {
		offset := pr.input.Position()
		for i := int32(0); i < length; i++ {
			start := pr.input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*bufutil.Int32SizeInBytes)
			if pr.input.Error() != nil {
				return nil, pr.input.Error()
			}
			pr.input.SetPosition(start)
			portables[i], err = pr.serializer.ReadObject(pr.input, factoryID, classID)
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
