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
	// hex: 0x014300
	MapSetTtlCodecRequestMessageType = int32(82688)
	// hex: 0x014301
	MapSetTtlCodecResponseMessageType = int32(82689)

	MapSetTtlCodecRequestTtlOffset        = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapSetTtlCodecRequestInitialFrameSize = MapSetTtlCodecRequestTtlOffset + proto.LongSizeInBytes

	MapSetTtlResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Updates TTL (time to live) value of the entry specified by {@code key} with a new TTL value.
// New TTL value is valid from this operation is invoked, not from the original creation of the entry.
// If the entry does not exist or already expired, then this call has no effect.
// <p>
// The entry will expire and get evicted after the TTL. If the TTL is 0,
// then the entry lives forever. If the TTL is negative, then the TTL
// from the map configuration will be used (default: forever).
//
// If there is no entry with key {@code key}, this call has no effect.
//
// <b>Warning:</b>
// <p>
// Time resolution for TTL is seconds. The given TTL value is rounded to the next closest second value.

func EncodeMapSetTtlRequest(name string, key serialization.Data, ttl int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapSetTtlCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSetTtlCodecRequestTtlOffset, ttl)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapSetTtlCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapSetTtlResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapSetTtlResponseResponseOffset)
}
