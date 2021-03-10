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
	// hex: 0x070200
	FencedLockTryLockCodecRequestMessageType = int32(459264)
	// hex: 0x070201
	FencedLockTryLockCodecResponseMessageType = int32(459265)

	FencedLockTryLockCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	FencedLockTryLockCodecRequestThreadIdOffset      = FencedLockTryLockCodecRequestSessionIdOffset + proto.LongSizeInBytes
	FencedLockTryLockCodecRequestInvocationUidOffset = FencedLockTryLockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	FencedLockTryLockCodecRequestTimeoutMsOffset     = FencedLockTryLockCodecRequestInvocationUidOffset + proto.UuidSizeInBytes
	FencedLockTryLockCodecRequestInitialFrameSize    = FencedLockTryLockCodecRequestTimeoutMsOffset + proto.LongSizeInBytes

	FencedLockTryLockResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Attempts to acquire the given FencedLock on the given CP group.
// If the lock is acquired, a valid fencing token (positive number) is
// returned. If not acquired either because of max reentrant entry limit or
// the lock is not free during the timeout duration, the call returns -1.
// If the lock is held by some other endpoint when this method is called,
// the caller thread is blocked until the lock is released or the timeout
// duration passes. If the session is closed between reentrant acquires,
// the call fails with {@code LockOwnershipLostException}.

func EncodeFencedLockTryLockRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID, timeoutMs int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, FencedLockTryLockCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockTryLockCodecRequestSessionIdOffset, sessionId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockTryLockCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, FencedLockTryLockCodecRequestInvocationUidOffset, invocationUid)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockTryLockCodecRequestTimeoutMsOffset, timeoutMs)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(FencedLockTryLockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeFencedLockTryLockResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, FencedLockTryLockResponseResponseOffset)
}
