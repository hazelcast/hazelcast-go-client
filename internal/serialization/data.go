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
	"encoding/binary"
	"math"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/murmur"
)

const (
	typeOffset       = 4
	DataOffset       = 8
	heapDataOverhead = 8
)

type SerializationData struct {
	Payload []byte
}

func (d *SerializationData) ToByteArray() []byte {
	return d.Payload
}

// NewData return serialization Data with the given payload.
func NewData(payload []byte) Data {
	return &SerializationData{payload}
}

func (d *SerializationData) Buffer() []byte {
	return d.Payload
}

func (d *SerializationData) GetType() int32 {
	if d.TotalSize() == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(d.Payload[typeOffset:]))
}

func (d *SerializationData) TotalSize() int {
	if d.Payload == nil {
		return 0
	}
	return len(d.Payload)
}

func (d *SerializationData) DataSize() int {
	return int(math.Max(float64(d.TotalSize()-heapDataOverhead), 0))
}

func (d *SerializationData) GetPartitionHash() int32 {
	return murmur.Default3A(d.Payload, DataOffset, d.DataSize())
}
