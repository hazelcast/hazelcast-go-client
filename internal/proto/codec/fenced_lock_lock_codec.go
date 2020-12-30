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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x070100
	FencedLockLockCodecRequestMessageType = int32(459008)
	// hex: 0x070101
	FencedLockLockCodecResponseMessageType = int32(459009)

	FencedLockLockCodecRequestSessionIdOffset     = proto.PartitionIDOffset + proto.IntSizeInBytes
	FencedLockLockCodecRequestThreadIdOffset      = FencedLockLockCodecRequestSessionIdOffset + proto.LongSizeInBytes
	FencedLockLockCodecRequestInvocationUidOffset = FencedLockLockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	FencedLockLockCodecRequestInitialFrameSize    = FencedLockLockCodecRequestInvocationUidOffset + proto.UuidSizeInBytes

	FencedLockLockResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Acquires the given FencedLock on the given CP group. If the lock is
// acquired, a valid fencing token (positive number) is returned. If not
// acquired because of max reentrant entry limit, the call returns -1.
// If the lock is held by some other endpoint when this method is called,
// the caller thread is blocked until the lock is released. If the session
// is closed between reentrant acquires, the call fails with
// {@code LockOwnershipLostException}.
type fencedlockLockCodec struct{}

var FencedLockLockCodec fencedlockLockCodec

func (fencedlockLockCodec) EncodeRequest(groupId proto.RaftGroupId, name string, sessionId int64, threadId int64, invocationUid core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, FencedLockLockCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockLockCodecRequestSessionIdOffset, sessionId)
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, FencedLockLockCodecRequestThreadIdOffset, threadId)
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, FencedLockLockCodecRequestInvocationUidOffset, invocationUid)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(FencedLockLockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.RaftGroupIdCodec.Encode(clientMessage, groupId)
	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (fencedlockLockCodec) DecodeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, FencedLockLockResponseResponseOffset)
}
