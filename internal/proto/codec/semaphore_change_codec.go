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
	// hex: 0x0C0500
	SemaphoreChangeCodecRequestMessageType = int32(787712)
	// hex: 0x0C0501
	SemaphoreChangeCodecResponseMessageType = int32(787713)

	SemaphoreChangeCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	SemaphoreChangeCodecRequestThreadIdOffset      = SemaphoreChangeCodecRequestSessionIdOffset + proto.LongSizeInBytes
	SemaphoreChangeCodecRequestInvocationUidOffset = SemaphoreChangeCodecRequestThreadIdOffset + proto.LongSizeInBytes
	SemaphoreChangeCodecRequestPermitsOffset       = SemaphoreChangeCodecRequestInvocationUidOffset + proto.UuidSizeInBytes
	SemaphoreChangeCodecRequestInitialFrameSize    = SemaphoreChangeCodecRequestPermitsOffset + proto.IntSizeInBytes

	SemaphoreChangeResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Increases or decreases the number of permits by the given value.
type semaphoreChangeCodec struct{}

var SemaphoreChangeCodec semaphoreChangeCodec

func (semaphoreChangeCodec) EncodeRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID, permits int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, SemaphoreChangeCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreChangeCodecRequestSessionIdOffset, sessionId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreChangeCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, SemaphoreChangeCodecRequestInvocationUidOffset, invocationUid)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SemaphoreChangeCodecRequestPermitsOffset, permits)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SemaphoreChangeCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)
	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (semaphoreChangeCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SemaphoreChangeResponseResponseOffset)
}
