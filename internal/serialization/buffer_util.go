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
	"math"
)

func WriteInt32(buf []byte, pos int32, v int32, bo binary.ByteOrder) {
	bo.PutUint32(buf[pos:], uint32(v))
}

func ReadInt32(buf []byte, pos int32, bo binary.ByteOrder) int32 {
	return int32(bo.Uint32(buf[pos:]))
}

func WriteFloat32(buf []byte, pos int32, v float32, bo binary.ByteOrder) {
	bo.PutUint32(buf[pos:], math.Float32bits(v))
}

func ReadFloat32(buf []byte, pos int32, bo binary.ByteOrder) float32 {
	return math.Float32frombits(bo.Uint32(buf[pos:]))
}

func WriteFloat64(buf []byte, pos int32, v float64, bo binary.ByteOrder) {
	bo.PutUint64(buf[pos:], math.Float64bits(v))
}

func ReadFloat64(buf []byte, pos int32, bo binary.ByteOrder) float64 {
	return math.Float64frombits(bo.Uint64(buf[pos:]))
}

func WriteBool(buf []byte, pos int32, v bool) {
	if v {
		buf[pos] = 1
	} else {
		buf[pos] = 0
	}
}

func ReadBool(buf []byte, pos int32) bool {
	return buf[pos] == 1
}

func WriteUInt16(buf []byte, pos int32, v uint16, bo binary.ByteOrder) {
	bo.PutUint16(buf[pos:], v)
}

func ReadUInt16(buf []byte, pos int32, bo binary.ByteOrder) uint16 {
	return bo.Uint16(buf[pos:])
}

func WriteInt16(buf []byte, pos int32, v int16, bo binary.ByteOrder) {
	bo.PutUint16(buf[pos:], uint16(v))
}

func ReadInt16(buf []byte, pos int32, bo binary.ByteOrder) int16 {
	return int16(bo.Uint16(buf[pos:]))
}

func WriteInt64(buf []byte, pos int32, v int64, bo binary.ByteOrder) {
	bo.PutUint64(buf[pos:], uint64(v))
}

func ReadInt64(buf []byte, pos int32, bo binary.ByteOrder) int64 {
	return int64(bo.Uint64(buf[pos:]))
}
