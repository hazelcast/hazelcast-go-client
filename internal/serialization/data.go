// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

const (
	TypeOffset       = 4
	DataOffset       = 8
	HeapDataOverhead = 8
)

type Data struct {
	Payload []byte
}

func NewData(payload []byte) *Data {
	return &Data{payload}
}

func (data Data) Buffer() []byte {
	return data.Payload
}

func (data Data) GetType() int32 {
	if data.TotalSize() == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(data.Payload[TypeOffset:]))
}

func (data Data) TotalSize() int {
	if data.Payload == nil {
		return 0
	}
	return len(data.Payload)
}

func (d Data) DataSize() int {
	return int(math.Max(float64(d.TotalSize()-HeapDataOverhead), 0))
}

func (d Data) GetPartitionHash() int32 {
	return common.Murmur3ADefault(d.Payload, DataOffset, d.DataSize())
}
