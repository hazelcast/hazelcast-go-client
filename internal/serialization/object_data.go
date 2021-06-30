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
	"unsafe"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
)

const (
	ByteSizeInBytes    = 1
	BoolSizeInBytes    = 1
	Uint8SizeInBytes   = 1
	Int16SizeInBytes   = 2
	Uint16SizeInBytes  = 2
	Int32SizeInBytes   = 4
	Float32SizeInBytes = 4
	Float64SizeInBytes = 8
	Int64SizeInBytes   = 8
	nilArrayLength     = -1
)

type ObjectDataOutput struct {
	bo       binary.ByteOrder
	service  *Service
	buffer   []byte
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
		return nil
	}
	snapBuffer := make([]byte, o.position)
	copy(snapBuffer, o.buffer)
	return snapBuffer
}

func (o *ObjectDataOutput) WriteZeroBytes(count int) {
	o.EnsureAvailable(count)
	for i := 0; i < count; i++ {
		o.writeByte(0)
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
	o.EnsureAvailable(ByteSizeInBytes)
	o.writeByte(v)
}

func (o *ObjectDataOutput) writeByte(v byte) {
	o.buffer[o.position] = v
	o.position += ByteSizeInBytes
}

func (o *ObjectDataOutput) WriteBool(v bool) {
	o.EnsureAvailable(BoolSizeInBytes)
	o.writeBool(v)
}

func (o *ObjectDataOutput) writeBool(v bool) {
	WriteBool(o.buffer, o.position, v)
	o.position += BoolSizeInBytes
}

func (o *ObjectDataOutput) WriteUInt16(v uint16) {
	o.EnsureAvailable(Uint16SizeInBytes)
	WriteUInt16(o.buffer, o.position, v, o.bo)
	o.position += Uint16SizeInBytes
}

func (o *ObjectDataOutput) WriteInt16(v int16) {
	o.EnsureAvailable(Int16SizeInBytes)
	WriteInt16(o.buffer, o.position, v, o.bo)
	o.position += Int16SizeInBytes
}

func (o *ObjectDataOutput) WriteInt32(v int32) {
	o.EnsureAvailable(Int32SizeInBytes)
	WriteInt32(o.buffer, o.position, v, o.bo)
	o.position += Int32SizeInBytes
}

func (o *ObjectDataOutput) WriteInt64(v int64) {
	o.EnsureAvailable(Int64SizeInBytes)
	WriteInt64(o.buffer, o.position, v, o.bo)
	o.position += Int64SizeInBytes
}

func (o *ObjectDataOutput) WriteFloat32(v float32) {
	o.EnsureAvailable(Float32SizeInBytes)
	WriteFloat32(o.buffer, o.position, v, o.bo)
	o.position += Float32SizeInBytes
}

func (o *ObjectDataOutput) WriteFloat64(v float64) {
	o.EnsureAvailable(Float64SizeInBytes)
	WriteFloat64(o.buffer, o.position, v, o.bo)
	o.position += Float64SizeInBytes
}

func (o *ObjectDataOutput) WriteString(v string) {
	length := len(v)
	o.WriteInt32(int32(length))
	if length > 0 {
		o.EnsureAvailable(length)
		copy(o.buffer[o.position:], v)
		o.position += int32(length)
	}
}

func (o *ObjectDataOutput) WriteObject(object interface{}) {
	o.service.WriteObject(o, object)
}

func (o *ObjectDataOutput) WriteByteArray(v []byte) {
	if v == nil {
		o.WriteInt32(nilArrayLength)
		return
	}
	length := len(v)
	o.WriteInt32(int32(length))
	o.EnsureAvailable(length)
	o.position += int32(copy(o.buffer[o.position:], v))
}

func (o *ObjectDataOutput) WriteBoolArray(v []bool) {
	if v == nil {
		o.WriteInt32(nilArrayLength)
		return
	}
	o.WriteInt32(int32(len(v)))
	o.EnsureAvailable(len(v) * BoolSizeInBytes)
	for _, b := range v {
		o.writeBool(b)
	}
}

func (o *ObjectDataOutput) WriteUInt16Array(v []uint16) {
	if v == nil {
		o.WriteInt32(nilArrayLength)
		return
	}
	o.WriteInt32(int32(len(v)))
	for j := 0; j < len(v); j++ {
		o.WriteUInt16(v[j])
	}
}

func (o *ObjectDataOutput) WriteInt16Array(v []int16) {
	if v == nil {
		o.WriteInt32(nilArrayLength)
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
		o.WriteInt32(nilArrayLength)
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
		o.WriteInt32(nilArrayLength)
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
		o.WriteInt32(nilArrayLength)
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
		o.WriteInt32(nilArrayLength)
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
		o.WriteInt32(nilArrayLength)
		return
	}
	o.WriteInt32(int32(len(v)))
	for j := 0; j < len(v); j++ {
		o.WriteString(v[j])
	}
}

func (o *ObjectDataOutput) WriteStringBytes(v string) {
	o.EnsureAvailable(ByteSizeInBytes * len(v))
	o.position += int32(copy(o.buffer[o.position:], v))
}

func (o *ObjectDataOutput) WriteRawBytes(b []byte) {
	o.EnsureAvailable(ByteSizeInBytes * len(b))
	o.position += int32(copy(o.buffer[o.position:], b))
}

//// ObjectDataInput ////

type ObjectDataInput struct {
	bo       binary.ByteOrder
	service  *Service
	buffer   []byte
	offset   int32
	position int32
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

func (i *ObjectDataInput) Available() int32 {
	return int32(len(i.buffer)) - i.position
}

func (i *ObjectDataInput) AssertAvailable(k int) {
	if i.position < 0 {
		panic(hzerrors.NewHazelcastIllegalArgumentError(fmt.Sprintf("negative pos: %v", i.position), nil))
	}
	if len(i.buffer) < int(i.position)+k {
		panic(hzerrors.NewHazelcastEOFError(fmt.Sprintf("cannot read %v bytes", k), nil))
	}
}

func (i *ObjectDataInput) Position() int32 {
	return i.position
}

func (i *ObjectDataInput) SetPosition(pos int32) {
	i.position = pos
}

func (i *ObjectDataInput) ReadByte() byte {
	i.AssertAvailable(ByteSizeInBytes)
	return i.readByte()
}

func (i *ObjectDataInput) readByte() byte {
	ret := i.buffer[i.position]
	i.position += ByteSizeInBytes
	return ret
}

func (i *ObjectDataInput) ReadByteAtPosition(pos int32) byte {
	return i.buffer[pos]
}

func (i *ObjectDataInput) ReadBool() bool {
	i.AssertAvailable(BoolSizeInBytes)
	return i.readBool()
}

func (i *ObjectDataInput) readBool() bool {
	ret := ReadBool(i.buffer, i.position)
	i.position += BoolSizeInBytes
	return ret
}

func (i *ObjectDataInput) ReadBoolAtPosition(pos int32) bool {
	return ReadBool(i.buffer, pos)
}

func (i *ObjectDataInput) ReadUInt16() uint16 {
	i.AssertAvailable(Uint16SizeInBytes)
	r := ReadUInt16(i.buffer, i.position, i.bo)
	i.position += Uint16SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadUInt16AtPosition(pos int32) uint16 {
	return ReadUInt16(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadInt16() int16 {
	i.AssertAvailable(Int16SizeInBytes)
	r := ReadInt16(i.buffer, i.position, i.bo)
	i.position += Int16SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadInt16AtPosition(pos int32) int16 {
	return ReadInt16(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadInt32() int32 {
	return i.readInt32()
}

func (i *ObjectDataInput) readInt32() int32 {
	i.AssertAvailable(Int32SizeInBytes)
	r := ReadInt32(i.buffer, i.position, i.bo)
	i.position += Int32SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadInt32AtPosition(pos int32) int32 {
	return ReadInt32(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadInt64() int64 {
	i.AssertAvailable(Int64SizeInBytes)
	r := ReadInt64(i.buffer, i.position, i.bo)
	i.position += Int64SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadInt64AtPosition(pos int32) int64 {
	return ReadInt64(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadFloat32() float32 {
	i.AssertAvailable(Float32SizeInBytes)
	r := ReadFloat32(i.buffer, i.position, i.bo)
	i.position += Float32SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadFloat32AtPosition(pos int32) float32 {
	return ReadFloat32(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadFloat64() float64 {
	i.AssertAvailable(Float64SizeInBytes)
	r := ReadFloat64(i.buffer, i.position, i.bo)
	i.position += Float64SizeInBytes
	return r
}

func (i *ObjectDataInput) ReadFloat64AtPosition(pos int32) float64 {
	return ReadFloat64(i.buffer, pos, i.bo)
}

func (i *ObjectDataInput) ReadString() string {
	size := i.readInt32()
	if size == nilArrayLength {
		return ""
	}
	s := byteSliceToString(i.buffer[i.position : i.position+size])
	i.position += size
	return s
}

func (i *ObjectDataInput) ReadStringAtPosition(pos int32) string {
	size := i.ReadInt32AtPosition(pos)
	if size == nilArrayLength {
		return ""
	}
	pos += Int32SizeInBytes
	s := byteSliceToString(i.buffer[pos : pos+size])
	pos += size
	return s
}

func (i *ObjectDataInput) ReadObject() interface{} {
	return i.service.ReadObject(i)
}

func (i *ObjectDataInput) ReadByteArray() []byte {
	length := i.readInt32()
	if length == nilArrayLength {
		return nil
	}
	arr := i.buffer[i.position : i.position+length]
	i.position += length
	return arr
}

func (i *ObjectDataInput) ReadByteArrayAtPosition(pos int32) []byte {
	backupPos := i.position
	i.position = pos
	arr := i.ReadByteArray()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadBoolArray() []bool {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]bool, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadBool()
	}
	return arr
}

func (i *ObjectDataInput) ReadBoolArrayAtPosition(pos int32) []bool {
	backupPos := i.position
	i.position = pos
	arr := i.ReadBoolArray()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadUInt16Array() []uint16 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]uint16, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadUInt16()
	}
	return arr
}

func (i *ObjectDataInput) ReadUInt16ArrayAtPosition(pos int32) []uint16 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadUInt16Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadInt16Array() []int16 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]int16, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadInt16()
	}
	return arr
}

func (i *ObjectDataInput) ReadInt16ArrayAtPosition(pos int32) []int16 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadInt16Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadInt32Array() []int32 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]int32, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadInt32()
	}
	return arr
}

func (i *ObjectDataInput) ReadInt32ArrayAtPosition(pos int32) []int32 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadInt32Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadInt64Array() []int64 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]int64, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadInt64()
	}
	return arr
}

func (i *ObjectDataInput) ReadInt64ArrayAtPosition(pos int32) []int64 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadInt64Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadFloat32Array() []float32 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]float32, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadFloat32()
	}
	return arr
}

func (i *ObjectDataInput) ReadFloat32ArrayAtPosition(pos int32) []float32 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadFloat32Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadFloat64Array() []float64 {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]float64, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadFloat64()
	}
	return arr
}

func (i *ObjectDataInput) ReadFloat64ArrayAtPosition(pos int32) []float64 {
	backupPos := i.position
	i.position = pos
	arr := i.ReadFloat64Array()
	i.position = backupPos
	return arr
}

func (i *ObjectDataInput) ReadStringArray() []string {
	length := int(i.readInt32())
	if length == nilArrayLength {
		return nil
	}
	arr := make([]string, length)
	for j := 0; j < length; j++ {
		arr[j] = i.ReadString()
	}
	return arr
}

func (i *ObjectDataInput) ReadStringArrayAtPosition(pos int32) []string {
	backupPos := i.position
	i.position = pos
	arr := i.ReadStringArray()
	i.position = backupPos
	return arr
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

// byteSliceToString converts a byte slice to string without memory copying.
// This method is unsafe and should be used with caution. The same approach
// is used by strings.Builder.
func byteSliceToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}
