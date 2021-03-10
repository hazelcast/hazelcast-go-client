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
	// hex: 0x0C0200
	SemaphoreAcquireCodecRequestMessageType = int32(786944)
	// hex: 0x0C0201
	SemaphoreAcquireCodecResponseMessageType = int32(786945)

	SemaphoreAcquireCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	SemaphoreAcquireCodecRequestThreadIdOffset      = SemaphoreAcquireCodecRequestSessionIdOffset + proto.LongSizeInBytes
	SemaphoreAcquireCodecRequestInvocationUidOffset = SemaphoreAcquireCodecRequestThreadIdOffset + proto.LongSizeInBytes
	SemaphoreAcquireCodecRequestPermitsOffset       = SemaphoreAcquireCodecRequestInvocationUidOffset + proto.UuidSizeInBytes
	SemaphoreAcquireCodecRequestTimeoutMsOffset     = SemaphoreAcquireCodecRequestPermitsOffset + proto.IntSizeInBytes
	SemaphoreAcquireCodecRequestInitialFrameSize    = SemaphoreAcquireCodecRequestTimeoutMsOffset + proto.LongSizeInBytes

	SemaphoreAcquireResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Acquires the requested amount of permits if available, reducing
// the number of available permits. If no enough permits are available,
// then the current thread becomes disabled for thread scheduling purposes
// and lies dormant until other threads release enough permits.

func EncodeSemaphoreAcquireRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID, permits int32, timeoutMs int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, SemaphoreAcquireCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreAcquireCodecRequestSessionIdOffset, sessionId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreAcquireCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, SemaphoreAcquireCodecRequestInvocationUidOffset, invocationUid)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SemaphoreAcquireCodecRequestPermitsOffset, permits)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SemaphoreAcquireCodecRequestTimeoutMsOffset, timeoutMs)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SemaphoreAcquireCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeSemaphoreAcquireResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SemaphoreAcquireResponseResponseOffset)
}
