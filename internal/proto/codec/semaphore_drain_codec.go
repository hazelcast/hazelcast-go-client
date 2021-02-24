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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x0C0400
	SemaphoreDrainCodecRequestMessageType = int32(787456)
	// hex: 0x0C0401
	SemaphoreDrainCodecResponseMessageType = int32(787457)

	SemaphoreDrainCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	SemaphoreDrainCodecRequestThreadIdOffset      = SemaphoreDrainCodecRequestSessionIdOffset + proto.LongSizeInBytes
	SemaphoreDrainCodecRequestInvocationUidOffset = SemaphoreDrainCodecRequestThreadIdOffset + proto.LongSizeInBytes
	SemaphoreDrainCodecRequestInitialFrameSize    = SemaphoreDrainCodecRequestInvocationUidOffset + proto.UuidSizeInBytes

	SemaphoreDrainResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Acquires all available permits at once and returns immediately.

func EncodeSemaphoreDrainRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, SemaphoreDrainCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreDrainCodecRequestSessionIdOffset, sessionId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreDrainCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, SemaphoreDrainCodecRequestInvocationUidOffset, invocationUid)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SemaphoreDrainCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeSemaphoreDrainResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeInt(initialFrame.Content, SemaphoreDrainResponseResponseOffset)
}
