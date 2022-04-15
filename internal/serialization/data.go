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

	"github.com/hazelcast/hazelcast-go-client/internal/murmur"
)

const (
	typeOffset       = 4
	DataOffset       = 8
	heapDataOverhead = 8
)

type Data []byte

func (d Data) ToByteArray() []byte {
	return d
}

func (d Data) Type() int32 {
	if d == nil {
		return TypeNil
	}
	return int32(binary.BigEndian.Uint32(d[typeOffset:]))
}

func (d Data) DataSize() int {
	v := len(d) - heapDataOverhead
	if v <= 0 {
		return 0
	}
	return v
}

func (d Data) PartitionHash() int32 {
	return murmur.Default3A(d, DataOffset, d.DataSize())
}
