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
	"encoding/binary"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type ObjectDataOutput struct {
	buffer   []byte
	service  *Service
	bo       binary.ByteOrder
	position int32
}

func NewObjectDataOutput(length int, service *Service, bigEndian bool) *ObjectDataOutput {
	var bo binary.ByteOrder = binary.LittleEndian
	if bigEndian {
		bo = binary.BigEndian
	}
	return &ObjectDataOutput{
		buffer:  make([]byte, length),
		service: service,
		bo:      bo,
	}
}

func (o *ObjectDataOutput) Available() int {
	if o.buffer == nil {
		return 0
	}
	return len(o.buffer) - int(o.position)
}

func (o *ObjectDataOutput) Position() int32 {
	return o.position
}

func (o *ObjectDataOutput) SetPosition(pos int32) {
	o.position = pos
}

func (o *ObjectDataOutput) ToBuffer() []byte {
	if o.position == 0 {
		return make([]byte, 0)
	}
	snapBuffer := make([]byte, o.position)
	copy(snapBuffer, o.buffer)
	return snapBuffer
}

func (o *ObjectDataOutput) WriteZeroBytes(count int) {
	for i := 0; i < count; i++ {
		// ignoring the error here
		o.WriteByte(0)
	}
}

func (o *ObjectDataOutput) EnsureAvailable(size int) {
	if o.Available() < size {
		temp := make([]byte, int(o.position)+size)
		copy(temp, o.buffer)
		o.buffer = temp
	}
}

func (o *ObjectDataOutput) WriteByte(v byte) {
	o.EnsureAvailable(bufutil.ByteSizeInBytes)
	o.buffer[o.position] = v
	o.position += bufutil.ByteSizeInBytes
}

func (o *ObjectDataOutput) WriteBool(v bool) {
	o.EnsureAvailable(bufutil.BoolSizeInBytes)
	WriteBool(o.buffer, o.position, v)
	o.position += bufutil.BoolSizeInBytes
}

func (o *ObjectDataOutput) WriteUInt16(v uint16) {
	o.EnsureAvailable(bufutil.Uint16SizeInBytes)
	WriteUInt16(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Uint16SizeInBytes
}

func (o *ObjectDataOutput) WriteInt16(v int16) {
	o.EnsureAvailable(bufutil.Int16SizeInBytes)
	WriteInt16(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Int16SizeInBytes
}

func (o *ObjectDataOutput) WriteInt32(v int32) {
	o.EnsureAvailable(bufutil.Int32SizeInBytes)
	WriteInt32(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Int32SizeInBytes
}

func (o *ObjectDataOutput) WriteInt64(v int64) {
	o.EnsureAvailable(bufutil.Int64SizeInBytes)
	WriteInt64(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Int64SizeInBytes
}

func (o *ObjectDataOutput) WriteFloat32(v float32) {
	o.EnsureAvailable(bufutil.Float32SizeInBytes)
	WriteFloat32(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Float32SizeInBytes
}

func (o *ObjectDataOutput) WriteFloat64(v float64) {
	o.EnsureAvailable(bufutil.Float64SizeInBytes)
	WriteFloat64(o.buffer, o.position, v, o.bo)
	o.position += bufutil.Float64SizeInBytes
}

func (o *ObjectDataOutput) WriteString(v string) {
	b := []byte(v)
	size := int32(len(b))
	o.WriteInt32(size)
	if len(b) > 0 {
		o.EnsureAvailable(len(b))
		copy(o.buffer[o.position:o.position+size], b)
		o.position += size
	}
}

func (o *ObjectDataOutput) WriteObject(object interface{}) error {
	return o.service.WriteObject(o, object)
}

func (o *ObjectDataOutput) WriteByteArray(v []byte) {
	if v != nil {
		o.WriteInt32(int32(len(v)))
		for _, b := range v {
			o.WriteByte(b)
		}
	} else {
		o.WriteInt32(bufutil.NilArrayLength)
	}

}

func (o *ObjectDataOutput) WriteBoolArray(v []bool) {
	if v != nil {
		o.WriteInt32(int32(len(v)))
		for _, b := range v {
			o.WriteBool(b)
		}
	} else {
		o.WriteInt32(bufutil.NilArrayLength)
	}
}

func (o *ObjectDataOutput) WriteUInt16Array(v []uint16) {
	if v != nil {
		o.WriteInt32(int32(len(v)))
		for j := 0; j < len(v); j++ {
			o.WriteUInt16(v[j])
		}
	} else {
		o.WriteInt32(bufutil.NilArrayLength)
	}
}

func (o *ObjectDataOutput) WriteInt16Array(v []int16) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	for j := 0; j < length; j++ {
		o.WriteInt16(v[j])
	}
}

func (o *ObjectDataOutput) WriteInt32Array(v []int32) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	for j := 0; j < length; j++ {
		o.WriteInt32(v[j])
	}
}

func (o *ObjectDataOutput) WriteInt64Array(v []int64) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	for j := 0; j < length; j++ {
		o.WriteInt64(v[j])
	}
}

func (o *ObjectDataOutput) WriteFloat32Array(v []float32) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	for j := 0; j < length; j++ {
		o.WriteFloat32(v[j])
	}
}

func (o *ObjectDataOutput) WriteFloat64Array(v []float64) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	for j := 0; j < length; j++ {
		o.WriteFloat64(v[j])
	}
}

func (o *ObjectDataOutput) WriteStringArray(v []string) {
	if v == nil {
		o.WriteInt32(bufutil.NilArrayLength)
		return
	}
	o.WriteInt32(int32(len(v)))
	for j := 0; j < len(v); j++ {
		o.WriteString(v[j])
	}
}

func (o *ObjectDataOutput) WriteBytes(v string) {
	for _, char := range v {
		o.WriteByte(uint8(char))
	}
}

//// ObjectDataInput ////

type ObjectDataInput struct {
	buffer   []byte
	offset   int32
	service  *Service
	bo       binary.ByteOrder
	position int32
	err      error
}

func NewObjectDataInput(buffer []byte, offset int32, service *Service, bigEndian bool) *ObjectDataInput {
	var bo binary.ByteOrder = binary.LittleEndian
	if bigEndian {
		bo = binary.BigEndian
	}
	return &ObjectDataInput{
		buffer:   buffer,
		offset:   offset,
		service:  service,
		bo:       bo,
		position: offset,
	}
}

func (i *ObjectDataInput) Error() error {
	return i.err
}

func (i *ObjectDataInput) Available() int32 {
	return int32(len(i.buffer)) - i.position
}

func (i *ObjectDataInput) AssertAvailable(k int) error {
	if i.position < 0 {
		return hzerror.NewHazelcastIllegalArgumentError(fmt.Sprintf("negative pos -> %v", i.position), nil)
	}
	if len(i.buffer) < int(i.position)+k {
		return hzerror.NewHazelcastSerializationError(fmt.Sprintf("cannot read %v bytes", k), nil)
	}
	return nil
}

func (i *ObjectDataInput) Position() int32 {
	return i.position
}

func (i *ObjectDataInput) SetPosition(pos int32) {
	i.position = pos
}

func (i *ObjectDataInput) ReadByte() byte {
	if i.err != nil {
		return 0
	}
	var ret byte
	ret, i.err = i.readByte()
	return ret
}

func (i *ObjectDataInput) readByte() (byte, error) {
	var err = i.AssertAvailable(bufutil.ByteSizeInBytes)
	var ret byte
	if err == nil {
		ret = i.buffer[i.position]
		i.position += bufutil.ByteSizeInBytes
	}
	return ret, err
}

func (i *ObjectDataInput) ReadByteWithPosition(pos int32) byte {
	if i.err != nil {
		return 0
	}
	var res byte
	res, i.err = i.readByteWithPosition(pos)
	return res
}

func (i *ObjectDataInput) readByteWithPosition(pos int32) (byte, error) {
	var err = i.AssertAvailable(bufutil.ByteSizeInBytes)
	var ret byte
	if err == nil {
		ret = i.buffer[pos]
	}
	return ret, err
}

func (i *ObjectDataInput) ReadBool() bool {
	if i.err != nil {
		return false
	}
	var ret bool
	ret, i.err = i.readBool()
	return ret
}

func (i *ObjectDataInput) readBool() (bool, error) {
	var err = i.AssertAvailable(bufutil.BoolSizeInBytes)
	var ret bool
	if err == nil {
		ret = ReadBool(i.buffer, i.position)
		i.position += bufutil.BoolSizeInBytes
	}
	return ret, err
}

func (i *ObjectDataInput) ReadBoolWithPosition(pos int32) bool {
	if i.err != nil {
		return false
	}
	var res bool
	res, i.err = i.readBoolWithPosition(pos)
	return res
}

func (i *ObjectDataInput) readBoolWithPosition(pos int32) (bool, error) {
	var err = i.AssertAvailable(bufutil.BoolSizeInBytes)
	var ret bool
	if err == nil {
		ret = ReadBool(i.buffer, pos)
	}
	return ret, err
}

func (i *ObjectDataInput) ReadUInt16() uint16 {
	if i.err != nil {
		return 0
	}
	var ret uint16
	ret, i.err = i.readUInt16()
	return ret
}

func (i *ObjectDataInput) readUInt16() (uint16, error) {
	var err = i.AssertAvailable(bufutil.Uint16SizeInBytes)
	var ret uint16
	if err == nil {
		ret = ReadUInt16(i.buffer, i.position, i.bo)
		i.position += bufutil.Uint16SizeInBytes
	}
	return ret, err
}

func (i *ObjectDataInput) ReadUInt16WithPosition(pos int32) uint16 {
	if i.err != nil {
		return 0
	}
	var res uint16
	res, i.err = i.readUInt16WithPosition(pos)
	return res
}

func (i *ObjectDataInput) readUInt16WithPosition(pos int32) (uint16, error) {
	var err = i.AssertAvailable(bufutil.Uint16SizeInBytes)
	var ret uint16
	if err == nil {
		ret = ReadUInt16(i.buffer, pos, i.bo)
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt16() int16 {
	if i.err != nil {
		return 0
	}
	var ret int16
	ret, i.err = i.readInt16()
	return ret
}

func (i *ObjectDataInput) readInt16() (int16, error) {
	var err = i.AssertAvailable(bufutil.Int16SizeInBytes)
	var ret int16
	if err == nil {
		ret = ReadInt16(i.buffer, i.position, i.bo)
		i.position += bufutil.Int16SizeInBytes
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt16WithPosition(pos int32) int16 {
	if i.err != nil {
		return 0
	}
	var ret int16
	ret, i.err = i.readInt16WithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readInt16WithPosition(pos int32) (int16, error) {
	var err = i.AssertAvailable(bufutil.Int16SizeInBytes)
	var ret int16
	if err == nil {
		ret = ReadInt16(i.buffer, pos, i.bo)
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt32() int32 {
	if i.err != nil {
		return 0
	}
	var ret int32
	ret, i.err = i.readInt32()
	return ret
}

func (i *ObjectDataInput) readInt32() (int32, error) {
	var err = i.AssertAvailable(bufutil.Int32SizeInBytes)
	var ret int32
	if err == nil {
		ret = ReadInt32(i.buffer, i.position, i.bo)
		i.position += bufutil.Int32SizeInBytes
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt32WithPosition(pos int32) int32 {
	if i.err != nil {
		return 0
	}
	var ret int32
	ret, i.err = i.readInt32WithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readInt32WithPosition(pos int32) (int32, error) {
	var err = i.AssertAvailable(bufutil.Int32SizeInBytes)
	var ret int32
	if err == nil {
		ret = ReadInt32(i.buffer, pos, i.bo)
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt64() int64 {
	if i.err != nil {
		return 0
	}
	var ret int64
	ret, i.err = i.readInt64()
	return ret
}

func (i *ObjectDataInput) readInt64() (int64, error) {
	var err = i.AssertAvailable(bufutil.Int64SizeInBytes)
	var ret int64
	if err == nil {
		ret = ReadInt64(i.buffer, i.position, i.bo)
		i.position += bufutil.Int64SizeInBytes
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadInt64WithPosition(pos int32) int64 {
	if i.err != nil {
		return 0
	}
	var ret int64
	ret, i.err = i.readInt64WithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readInt64WithPosition(pos int32) (int64, error) {
	var err = i.AssertAvailable(bufutil.Int64SizeInBytes)
	var ret int64
	if err == nil {
		ret = ReadInt64(i.buffer, pos, i.bo)
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadFloat32() float32 {
	if i.err != nil {
		return 0
	}
	var ret float32
	ret, i.err = i.readFloat32()
	return ret
}

func (i *ObjectDataInput) readFloat32() (float32, error) {
	var err = i.AssertAvailable(bufutil.Float32SizeInBytes)
	var ret float32
	if err == nil {
		ret = ReadFloat32(i.buffer, i.position, i.bo)
		i.position += bufutil.Float32SizeInBytes
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadFloat32WithPosition(pos int32) float32 {
	if i.err != nil {
		return 0
	}
	var ret float32
	ret, i.err = i.readFloat32WithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readFloat32WithPosition(pos int32) (float32, error) {
	var err = i.AssertAvailable(bufutil.Float32SizeInBytes)
	var ret float32
	if err == nil {
		ret = ReadFloat32(i.buffer, pos, i.bo)
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadFloat64() float64 {
	if i.err != nil {
		return 0
	}
	var ret float64
	ret, i.err = i.readFloat64()
	return ret
}

func (i *ObjectDataInput) readFloat64() (float64, error) {
	var err = i.AssertAvailable(bufutil.Float64SizeInBytes)
	var ret float64
	if err == nil {
		ret = ReadFloat64(i.buffer, i.position, i.bo)
		i.position += bufutil.Float64SizeInBytes
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadFloat64WithPosition(pos int32) float64 {
	if i.err != nil {
		return 0
	}
	var ret float64
	ret, i.err = i.readFloat64WithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readFloat64WithPosition(pos int32) (float64, error) {
	var err = i.AssertAvailable(bufutil.Float64SizeInBytes)
	var ret float64
	if err == nil {
		ret = ReadFloat64(i.buffer, pos, i.bo)
		return ret, err
	}
	return ret, err
}

func (i *ObjectDataInput) ReadString() string {
	if i.err != nil {
		return ""
	}
	size, err := i.readInt32()
	if err != nil || size == bufutil.NilArrayLength {
		i.err = err
		return ""
	}
	s := string(i.buffer[i.position : i.position+size])
	i.position += size
	return s
}

func (i *ObjectDataInput) ReadUTFWithPosition(pos int32) string {
	if i.err != nil {
		return ""
	}
	var ret string
	ret, i.err = i.readUTFWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readUTFWithPosition(pos int32) (string, error) {
	size := i.ReadInt32WithPosition(pos)
	if i.err != nil || size == bufutil.NilArrayLength {
		return "", i.err
	}
	pos += bufutil.Int32SizeInBytes
	s := string(i.buffer[pos : pos+size])
	pos += size
	return s, nil
}

func (i *ObjectDataInput) ReadObject() interface{} {
	if i.err != nil {
		return nil
	}
	var ret interface{}
	ret, i.err = i.readObject()
	return ret
}

func (i *ObjectDataInput) readObject() (interface{}, error) {
	return i.service.ReadObject(i)
}

func (i *ObjectDataInput) ReadByteArray() []byte {
	if i.err != nil {
		return nil
	}
	var ret []byte
	ret, i.err = i.readByteArray()
	return ret
}

func (i *ObjectDataInput) readByteArray() ([]byte, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]byte, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadByte()
	}
	return arr, nil
}

func (i *ObjectDataInput) ReadByteArrayWithPosition(pos int32) []byte {
	if i.err != nil {
		return nil
	}
	var ret []byte
	ret, i.err = i.readByteArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readByteArrayWithPosition(pos int32) ([]byte, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]byte, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadByte()
	}
	i.position = backupPos
	return arr, nil
}

func (i *ObjectDataInput) ReadBoolArray() []bool {
	if i.err != nil {
		return nil
	}
	var ret []bool
	ret, i.err = i.readBoolArray()
	return ret
}

func (i *ObjectDataInput) readBoolArray() ([]bool, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]bool, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadBool()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadBoolArrayWithPosition(pos int32) []bool {
	if i.err != nil {
		return nil
	}
	var ret []bool
	ret, i.err = i.readBoolArrayWithPosition(pos)
	return ret

}

func (i *ObjectDataInput) readBoolArrayWithPosition(pos int32) ([]bool, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]bool, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadBool()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadUInt16Array() []uint16 {
	if i.err != nil {
		return nil
	}
	var ret []uint16
	ret, i.err = i.readUInt16Array()
	return ret
}

func (i *ObjectDataInput) readUInt16Array() ([]uint16, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]uint16, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadUInt16()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadUInt16ArrayWithPosition(pos int32) []uint16 {
	if i.err != nil {
		return nil
	}
	var ret []uint16
	ret, i.err = i.readUInt16ArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readUInt16ArrayWithPosition(pos int32) ([]uint16, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]uint16, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadUInt16()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt16Array() []int16 {
	if i.err != nil {
		return nil
	}
	var ret []int16
	ret, i.err = i.readInt16Array()
	return ret
}

func (i *ObjectDataInput) readInt16Array() ([]int16, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int16, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt16()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt16ArrayWithPosition(pos int32) []int16 {
	if i.err != nil {
		return nil
	}
	var ret []int16
	ret, i.err = i.readInt16ArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readInt16ArrayWithPosition(pos int32) ([]int16, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int16, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt16()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt32Array() []int32 {
	if i.err != nil {
		return nil
	}
	var ret []int32
	ret, i.err = i.readInt32Array()
	return ret
}

func (i *ObjectDataInput) readInt32Array() ([]int32, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int32, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt32()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt32ArrayWithPosition(pos int32) []int32 {
	if i.err != nil {
		return nil
	}
	var ret []int32
	ret, i.err = i.readInt32ArrayWithPosition(pos)
	return ret

}

func (i *ObjectDataInput) readInt32ArrayWithPosition(pos int32) ([]int32, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int32, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt32()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt64Array() []int64 {
	if i.err != nil {
		return nil
	}
	var ret []int64
	ret, i.err = i.readInt64Array()
	return ret
}

func (i *ObjectDataInput) readInt64Array() ([]int64, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int64, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt64()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadInt64ArrayWithPosition(pos int32) []int64 {
	if i.err != nil {
		return nil
	}
	var ret []int64
	ret, i.err = i.readInt64ArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readInt64ArrayWithPosition(pos int32) ([]int64, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]int64, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadInt64()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadFloat32Array() []float32 {
	if i.err != nil {
		return nil
	}
	var ret []float32
	ret, i.err = i.readFloat32Array()
	return ret
}

func (i *ObjectDataInput) readFloat32Array() ([]float32, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]float32, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadFloat32()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadFloat32ArrayWithPosition(pos int32) []float32 {
	if i.err != nil {
		return nil
	}
	var ret []float32
	ret, i.err = i.readFloat32ArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readFloat32ArrayWithPosition(pos int32) ([]float32, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]float32, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadFloat32()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadFloat64Array() []float64 {
	if i.err != nil {
		return nil
	}
	var ret []float64
	ret, i.err = i.readFloat64Array()
	return ret
}

func (i *ObjectDataInput) readFloat64Array() ([]float64, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]float64, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadFloat64()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadFloat64ArrayWithPosition(pos int32) []float64 {
	if i.err != nil {
		return nil
	}
	var ret []float64
	ret, i.err = i.readFloat64ArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readFloat64ArrayWithPosition(pos int32) ([]float64, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]float64, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadFloat64()
	}
	i.position = backupPos
	return arr, i.err
}

func (i *ObjectDataInput) ReadUTFArray() []string {
	if i.err != nil {
		return nil
	}
	var ret []string
	ret, i.err = i.readUTFArray()
	return ret
}

func (i *ObjectDataInput) readUTFArray() ([]string, error) {
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]string, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadString()
	}
	return arr, i.err
}

func (i *ObjectDataInput) ReadUTFArrayWithPosition(pos int32) []string {
	if i.err != nil {
		return nil
	}
	var ret []string
	ret, i.err = i.readUTFArrayWithPosition(pos)
	return ret
}

func (i *ObjectDataInput) readUTFArrayWithPosition(pos int32) ([]string, error) {
	backupPos := i.position
	i.position = pos
	length, err := i.readInt32()
	if err != nil || length == bufutil.NilArrayLength {
		return nil, err
	}
	var arr = make([]string, length)
	for j := int32(0); j < length; j++ {
		arr[j] = i.ReadString()
	}
	i.position = backupPos
	return arr, i.err
}

type PositionalObjectDataOutput struct {
	*ObjectDataOutput
}

func NewPositionalObjectDataOutput(length int, service *Service, bigEndian bool) *PositionalObjectDataOutput {
	return &PositionalObjectDataOutput{NewObjectDataOutput(length, service, bigEndian)}
}

func (p *PositionalObjectDataOutput) PWriteByte(pos int32, v byte) {
	p.buffer[pos] = v
}

func (p *PositionalObjectDataOutput) PWriteBool(pos int32, v bool) {
	WriteBool(p.buffer, pos, v)
}

func (p *PositionalObjectDataOutput) PWriteUInt16(pos int32, v uint16) {
	WriteUInt16(p.buffer, pos, v, p.bo)
}

func (p *PositionalObjectDataOutput) PWriteInt16(pos int32, v int16) {
	WriteInt16(p.buffer, pos, v, p.bo)
}

func (p *PositionalObjectDataOutput) PWriteInt32(pos int32, v int32) {
	WriteInt32(p.buffer, pos, v, p.bo)
}

func (p *PositionalObjectDataOutput) PWriteInt64(pos int32, v int64) {
	WriteInt64(p.buffer, pos, v, p.bo)
}

func (p *PositionalObjectDataOutput) PWriteFloat32(pos int32, v float32) {
	WriteFloat32(p.buffer, pos, v, p.bo)
}

func (p *PositionalObjectDataOutput) PWriteFloat64(pos int32, v float64) {
	WriteFloat64(p.buffer, pos, v, p.bo)
}
