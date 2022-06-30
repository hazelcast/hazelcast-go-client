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

package nearcache

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	EventIMapInvalidationMessageType              = 81666
	EventIMapBatchInvalidationMessageType         = 81667
	eventIMapInvalidationSourceUUIDFieldOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	eventIMapInvalidationPartitionUUIDFieldOffset = eventIMapInvalidationSourceUUIDFieldOffset + proto.UUIDSizeInBytes
	eventIMapInvalidationSequenceFieldOffset      = eventIMapInvalidationPartitionUUIDFieldOffset + proto.UUIDSizeInBytes
)

func DecodeInvalidationMsg(msg *proto.ClientMessage) (key serialization.Data, source types.UUID, partition types.UUID, seq int64) {
	it := msg.FrameIterator()
	frame := it.Next()
	source = codec.FixSizedTypesCodec.DecodeUUID(frame.Content, eventIMapInvalidationSourceUUIDFieldOffset)
	partition = codec.FixSizedTypesCodec.DecodeUUID(frame.Content, eventIMapInvalidationPartitionUUIDFieldOffset)
	seq = codec.FixSizedTypesCodec.DecodeLong(frame.Content, eventIMapInvalidationSequenceFieldOffset)
	key = codec.DecodeNullableData(it)
	return
}
