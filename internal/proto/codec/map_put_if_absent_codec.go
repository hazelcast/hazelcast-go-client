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
	// hex: 0x010E00
	MapPutIfAbsentCodecRequestMessageType = int32(69120)
	// hex: 0x010E01
	MapPutIfAbsentCodecResponseMessageType = int32(69121)

	MapPutIfAbsentCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutIfAbsentCodecRequestTtlOffset        = MapPutIfAbsentCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapPutIfAbsentCodecRequestInitialFrameSize = MapPutIfAbsentCodecRequestTtlOffset + proto.LongSizeInBytes
)

// Puts an entry into this map with a given ttl (time to live) value if the specified key is not already associated
// with a value. Entry will expire and get evicted after the ttl.

func EncodeMapPutIfAbsentRequest(name string, key serialization.Data, value serialization.Data, threadId int64, ttl int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapPutIfAbsentCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutIfAbsentCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutIfAbsentCodecRequestTtlOffset, ttl)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutIfAbsentCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapPutIfAbsentResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
