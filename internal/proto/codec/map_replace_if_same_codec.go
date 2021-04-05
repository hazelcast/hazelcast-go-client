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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x010500
	MapReplaceIfSameCodecRequestMessageType = int32(66816)
	// hex: 0x010501
	MapReplaceIfSameCodecResponseMessageType = int32(66817)

	MapReplaceIfSameCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapReplaceIfSameCodecRequestInitialFrameSize = MapReplaceIfSameCodecRequestThreadIdOffset + proto.LongSizeInBytes

	MapReplaceIfSameResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Replaces the the entry for a key only if existing values equal to the testValue

func EncodeMapReplaceIfSameRequest(name string, key serialization.Data, testValue serialization.Data, value serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapReplaceIfSameCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapReplaceIfSameCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapReplaceIfSameCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, testValue)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMapReplaceIfSameResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapReplaceIfSameResponseResponseOffset)
}
