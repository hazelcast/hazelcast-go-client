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
	// hex: 0x070300
	FencedLockUnlockCodecRequestMessageType = int32(459520)
	// hex: 0x070301
	FencedLockUnlockCodecResponseMessageType = int32(459521)

	FencedLockUnlockCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	FencedLockUnlockCodecRequestThreadIdOffset      = FencedLockUnlockCodecRequestSessionIdOffset + proto.LongSizeInBytes
	FencedLockUnlockCodecRequestInvocationUidOffset = FencedLockUnlockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	FencedLockUnlockCodecRequestInitialFrameSize    = FencedLockUnlockCodecRequestInvocationUidOffset + proto.UuidSizeInBytes

	FencedLockUnlockResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Unlocks the given FencedLock on the given CP group. If the lock is
// not acquired, the call fails with {@link IllegalMonitorStateException}.
// If the session is closed while holding the lock, the call fails with
// {@code LockOwnershipLostException}. Returns true if the lock is still
// held by the caller after a successful unlock() call, false otherwise.

func EncodeFencedLockUnlockRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, FencedLockUnlockCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockUnlockCodecRequestSessionIdOffset, sessionId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockUnlockCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, FencedLockUnlockCodecRequestInvocationUidOffset, invocationUid)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(FencedLockUnlockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeFencedLockUnlockResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, FencedLockUnlockResponseResponseOffset)
}
