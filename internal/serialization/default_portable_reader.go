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

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type DefaultPortableReader struct {
	serializer      *PortableSerializer
	input           serialization.DataInput
	classDefinition *serialization.ClassDefinition
	offset          int32
	finalPos        int32
}

func NewDefaultPortableReader(serializer *PortableSerializer, input serialization.DataInput,
	classdefinition *serialization.ClassDefinition) *DefaultPortableReader {
	finalPos := input.ReadInt32()
	input.ReadInt32()
	offset := input.Position()
	return &DefaultPortableReader{
		serializer:      serializer,
		input:           input,
		classDefinition: classdefinition,
		offset:          offset,
		finalPos:        finalPos,
	}
}

func TypeByID(fieldType int32) string {
	switch t := fieldType; t {
	case serialization.TypePortable:
		return "Portable"
	case serialization.TypeByte:
		return "byte"
	case serialization.TypeBool:
		return "bool"
	case serialization.TypeUint16:
		return "uint16"
	case serialization.TypeInt16:
		return "int16"
	case serialization.TypeInt32:
		return "int32"
	case serialization.TypeInt64:
		return "int64"
	case serialization.TypeFloat32:
		return "float32"
	case serialization.TypeFloat64:
		return "float64"
	case serialization.TypeString:
		return "string"
	case serialization.TypePortableArray:
		return "[]Portable"
	case serialization.TypeByteArray:
		return "[]byte"
	case serialization.TypeBoolArray:
		return "[]bool"
	case serialization.TypeUint16Array:
		return "[]uint16"
	case serialization.TypeInt16Array:
		return "[]int16"
	case serialization.TypeInt32Array:
		return "[]int32"
	case serialization.TypeInt64Array:
		return "[]int64"
	case serialization.TypeFloat32Array:
		return "[]float32"
	case serialization.TypeFloat64Array:
		return "[]float64"
	case serialization.TypeStringArray:
		return "[]string"
	}
	return "UNKNOWN"
}

func (pr *DefaultPortableReader) positionByField(fieldName string, fieldType int32) int32 {
	field, ok := pr.classDefinition.Fields[fieldName]
	if !ok {
		panic(hzerrors.NewHazelcastSerializationError(fmt.Sprintf("unknown field: %s", fieldName), nil))
	}
	if field.Type != fieldType {
		panic(hzerrors.NewHazelcastSerializationError(fmt.Sprintf("not a %s field: %s", TypeByID(fieldType), fieldName), nil))
	}
	pos := pr.input.(*ObjectDataInput).ReadInt32AtPosition(pr.offset + field.Index*Int32SizeInBytes)
	length := pr.input.(*ObjectDataInput).ReadInt16AtPosition(pos)
	return pos + Int16SizeInBytes + int32(length) + 1
}

func (pr *DefaultPortableReader) ReadByte(fieldName string) byte {
	return pr.readByte(fieldName)
}

func (pr *DefaultPortableReader) readByte(fieldName string) byte {
	pos := pr.positionByField(fieldName, serialization.TypeByte)
	return pr.input.(*ObjectDataInput).ReadByteAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadBool(fieldName string) bool {
	return pr.readBool(fieldName)
}

func (pr *DefaultPortableReader) readBool(fieldName string) bool {
	pos := pr.positionByField(fieldName, serialization.TypeBool)
	return pr.input.(*ObjectDataInput).ReadBoolAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadUInt16(fieldName string) uint16 {
	return pr.readUInt16(fieldName)
}

func (pr *DefaultPortableReader) readUInt16(fieldName string) uint16 {
	pos := pr.positionByField(fieldName, serialization.TypeUint16)
	return pr.input.(*ObjectDataInput).ReadUInt16AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt16(fieldName string) int16 {
	return pr.readInt16(fieldName)
}

func (pr *DefaultPortableReader) readInt16(fieldName string) int16 {
	pos := pr.positionByField(fieldName, serialization.TypeInt16)
	return pr.input.(*ObjectDataInput).ReadInt16AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt32(fieldName string) int32 {
	return pr.readInt32(fieldName)
}

func (pr *DefaultPortableReader) readInt32(fieldName string) int32 {
	pos := pr.positionByField(fieldName, serialization.TypeInt32)
	return pr.input.(*ObjectDataInput).ReadInt32AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt64(fieldName string) int64 {
	return pr.readInt64(fieldName)
}

func (pr *DefaultPortableReader) readInt64(fieldName string) int64 {
	pos := pr.positionByField(fieldName, serialization.TypeInt64)
	return pr.input.(*ObjectDataInput).ReadInt64AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat32(fieldName string) float32 {
	return pr.readFloat32(fieldName)
}

func (pr *DefaultPortableReader) readFloat32(fieldName string) float32 {
	pos := pr.positionByField(fieldName, serialization.TypeFloat32)
	return pr.input.(*ObjectDataInput).ReadFloat32AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat64(fieldName string) float64 {
	return pr.readFloat64(fieldName)
}

func (pr *DefaultPortableReader) readFloat64(fieldName string) float64 {
	pos := pr.positionByField(fieldName, serialization.TypeFloat64)
	return pr.input.(*ObjectDataInput).ReadFloat64AtPosition(pos)
}

func (pr *DefaultPortableReader) ReadString(fieldName string) string {
	return pr.readUTF(fieldName)
}

func (pr *DefaultPortableReader) readUTF(fieldName string) string {
	pos := pr.positionByField(fieldName, serialization.TypeString)
	return pr.input.(*ObjectDataInput).ReadStringAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadPortable(fieldName string) serialization.Portable {
	return pr.readPortable(fieldName)
}

func (pr *DefaultPortableReader) readPortable(fieldName string) serialization.Portable {
	backupPos := pr.input.Position()
	pos := pr.positionByField(fieldName, serialization.TypePortable)
	pr.input.SetPosition(pos)
	isNil := pr.input.ReadBool()
	var r serialization.Portable
	if !isNil {
		factoryID := pr.input.ReadInt32()
		classID := pr.input.ReadInt32()
		r = pr.serializer.ReadObject(pr.input, factoryID, classID)
	}
	pr.input.SetPosition(backupPos)
	return r
}

func (pr *DefaultPortableReader) ReadByteArray(fieldName string) []byte {
	return pr.readByteArray(fieldName)
}

func (pr *DefaultPortableReader) readByteArray(fieldName string) []byte {
	pos := pr.positionByField(fieldName, serialization.TypeByteArray)
	return pr.input.(*ObjectDataInput).ReadByteArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadBoolArray(fieldName string) []bool {
	return pr.readBoolArray(fieldName)
}

func (pr *DefaultPortableReader) readBoolArray(fieldName string) []bool {
	pos := pr.positionByField(fieldName, serialization.TypeBoolArray)
	return pr.input.(*ObjectDataInput).ReadBoolArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadUInt16Array(fieldName string) []uint16 {
	return pr.readUInt16Array(fieldName)
}

func (pr *DefaultPortableReader) readUInt16Array(fieldName string) []uint16 {
	pos := pr.positionByField(fieldName, serialization.TypeUint16Array)
	return pr.input.(*ObjectDataInput).ReadUInt16ArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt16Array(fieldName string) []int16 {
	return pr.readInt16Array(fieldName)
}

func (pr *DefaultPortableReader) readInt16Array(fieldName string) []int16 {
	pos := pr.positionByField(fieldName, serialization.TypeInt16Array)
	return pr.input.(*ObjectDataInput).ReadInt16ArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt32Array(fieldName string) []int32 {
	return pr.readInt32Array(fieldName)
}

func (pr *DefaultPortableReader) readInt32Array(fieldName string) []int32 {
	pos := pr.positionByField(fieldName, serialization.TypeInt32Array)
	return pr.input.(*ObjectDataInput).ReadInt32ArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadInt64Array(fieldName string) []int64 {
	return pr.readInt64Array(fieldName)
}

func (pr *DefaultPortableReader) readInt64Array(fieldName string) []int64 {
	pos := pr.positionByField(fieldName, serialization.TypeInt64Array)
	return pr.input.(*ObjectDataInput).ReadInt64ArrayAtPosition(pos)
}
func (pr *DefaultPortableReader) ReadFloat32Array(fieldName string) []float32 {
	return pr.readFloat32Array(fieldName)
}

func (pr *DefaultPortableReader) readFloat32Array(fieldName string) []float32 {
	pos := pr.positionByField(fieldName, serialization.TypeFloat32Array)
	return pr.input.(*ObjectDataInput).ReadFloat32ArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadFloat64Array(fieldName string) []float64 {
	return pr.readFloat64Array(fieldName)
}

func (pr *DefaultPortableReader) readFloat64Array(fieldName string) []float64 {
	pos := pr.positionByField(fieldName, serialization.TypeFloat64Array)
	return pr.input.(*ObjectDataInput).ReadFloat64ArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadStringArray(fieldName string) []string {
	return pr.readUTFArray(fieldName)
}

func (pr *DefaultPortableReader) readUTFArray(fieldName string) []string {
	pos := pr.positionByField(fieldName, serialization.TypeStringArray)
	return pr.input.(*ObjectDataInput).ReadStringArrayAtPosition(pos)
}

func (pr *DefaultPortableReader) ReadPortableArray(fieldName string) []serialization.Portable {
	return pr.readPortableArray(fieldName)
}

func (pr *DefaultPortableReader) readPortableArray(fieldName string) []serialization.Portable {
	backupPos := pr.input.Position()
	pos := pr.positionByField(fieldName, serialization.TypePortableArray)
	pr.input.SetPosition(pos)
	length := pr.input.ReadInt32()
	factoryID := pr.input.ReadInt32()
	classID := pr.input.ReadInt32()
	var portables []serialization.Portable
	if length > 0 {
		portables = make([]serialization.Portable, length)
		offset := pr.input.Position()
		for i := int32(0); i < length; i++ {
			start := pr.input.(*ObjectDataInput).ReadInt32AtPosition(offset + i*Int32SizeInBytes)
			pr.input.SetPosition(start)
			portables[i] = pr.serializer.ReadObject(pr.input, factoryID, classID)
		}
	}
	pr.input.SetPosition(backupPos)
	return portables
}

func (pr *DefaultPortableReader) End() {
	pr.input.SetPosition(pr.finalPos)
}
