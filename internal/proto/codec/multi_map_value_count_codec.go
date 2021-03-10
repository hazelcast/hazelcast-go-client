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
	// hex: 0x020C00
	MultiMapValueCountCodecRequestMessageType = int32(134144)
	// hex: 0x020C01
	MultiMapValueCountCodecResponseMessageType = int32(134145)

	MultiMapValueCountCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapValueCountCodecRequestInitialFrameSize = MultiMapValueCountCodecRequestThreadIdOffset + proto.LongSizeInBytes

	MultiMapValueCountResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the number of values that match the given key in the multimap.

func EncodeMultiMapValueCountRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MultiMapValueCountCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapValueCountCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapValueCountCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMultiMapValueCountResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeInt(initialFrame.Content, MultiMapValueCountResponseResponseOffset)
}
