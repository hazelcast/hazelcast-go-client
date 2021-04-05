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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x011D00
	MapGetEntryViewCodecRequestMessageType = int32(72960)
	// hex: 0x011D01
	MapGetEntryViewCodecResponseMessageType = int32(72961)

	MapGetEntryViewCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapGetEntryViewCodecRequestInitialFrameSize = MapGetEntryViewCodecRequestThreadIdOffset + proto.LongSizeInBytes

	MapGetEntryViewResponseMaxIdleOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the EntryView for the specified key.
// This method returns a clone of original mapping, modifying the returned value does not change the actual value
// in the map. One should put modified value back to make changes visible to all nodes.

func EncodeMapGetEntryViewRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapGetEntryViewCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapGetEntryViewCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapGetEntryViewCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapGetEntryViewResponse(clientMessage *proto.ClientMessage) (response *hztypes.SimpleEntryView, maxIdle int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	maxIdle = FixSizedTypesCodec.DecodeLong(initialFrame.Content, MapGetEntryViewResponseMaxIdleOffset)
	response = CodecUtil.DecodeNullableForSimpleEntryView(frameIterator)

	return response, maxIdle
}
