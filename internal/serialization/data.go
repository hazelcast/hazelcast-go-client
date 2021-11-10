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

	"github.com/hazelcast/hazelcast-go-client/internal/murmur"
)

const (
	typeOffset       = 4
	DataOffset       = 8
	heapDataOverhead = 8
)

type Data struct {
	Payload []byte
}

func (d *Data) ToByteArray() []byte {
	return d.Payload
}

// NewData returns serialization Data with the given payload.
// Ownership of Payload is transferred, so it mustn't be used after passed to NewData
func NewData(payload []byte) *Data {
	return &Data{payload}
}

func (d *Data) Buffer() []byte {
	return d.Payload
}

func (d *Data) Type() int32 {
	if d.TotalSize() == 0 {
		return TypeNil
	}
	return int32(binary.BigEndian.Uint32(d.Payload[typeOffset:]))
}

func (d *Data) TotalSize() int {
	return len(d.Payload)
}

func (d *Data) DataSize() int {
	// TODO: Remove conversion to float64, amd math.Max
	return int(math.Max(float64(d.TotalSize()-heapDataOverhead), 0))
}

func (d *Data) PartitionHash() int32 {
	return murmur.Default3A(d.Payload, DataOffset, d.DataSize())
}
