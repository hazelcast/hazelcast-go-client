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
)

const (
	// hex: 0x0C0100
	SemaphoreInitCodecRequestMessageType = int32(786688)
	// hex: 0x0C0101
	SemaphoreInitCodecResponseMessageType = int32(786689)

	SemaphoreInitCodecRequestPermitsOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	SemaphoreInitCodecRequestInitialFrameSize = SemaphoreInitCodecRequestPermitsOffset + proto.IntSizeInBytes

	SemaphoreInitResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Initializes the ISemaphore instance with the given permit number, if not
// initialized before.

func EncodeSemaphoreInitRequest(groupId proto.RaftGroupId, name string, permits int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, SemaphoreInitCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SemaphoreInitCodecRequestPermitsOffset, permits)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SemaphoreInitCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeSemaphoreInitResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SemaphoreInitResponseResponseOffset)
}
