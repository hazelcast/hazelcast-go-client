// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x010F00
	MapSetCodecRequestMessageType = int32(69376)
	// hex: 0x010F01
	MapSetCodecResponseMessageType = int32(69377)

	MapSetCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapSetCodecRequestTtlOffset        = MapSetCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapSetCodecRequestInitialFrameSize = MapSetCodecRequestTtlOffset + proto.LongSizeInBytes
)

// Puts an entry into this map with a given ttl (time to live) value.Entry will expire and get evicted after the ttl
// If ttl is 0, then the entry lives forever. Similar to the put operation except that set doesn't
// return the old value, which is more efficient.
type mapSetCodec struct{}

var MapSetCodec mapSetCodec

func (mapSetCodec) EncodeRequest(name string, key serialization.Data, value serialization.Data, threadId int64, ttl int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapSetCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetCodecRequestTtlOffset, ttl)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, key)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}
