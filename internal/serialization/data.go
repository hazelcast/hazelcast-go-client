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

	"github.com/hazelcast/hazelcast-go-client/internal/util/murmur"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	typeOffset       = 4
	dataOffset       = 8
	heapDataOverhead = 8
)

func DataTypeFor(d pubserialization.Data) int32 {
	if len(d) == 0 {
		// nil
		return 0
	}
	return int32(binary.BigEndian.Uint32(d[typeOffset:]))
}

func DataPartitionHashFor(d pubserialization.Data) int32 {
	ds := len(d) - heapDataOverhead
	if ds < 0 {
		ds = 0
	}
	return murmur.Default3A(d, dataOffset, ds)
}
