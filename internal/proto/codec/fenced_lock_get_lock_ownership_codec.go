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
	// hex: 0x070400
	FencedLockGetLockOwnershipCodecRequestMessageType = int32(459776)
	// hex: 0x070401
	FencedLockGetLockOwnershipCodecResponseMessageType = int32(459777)

	FencedLockGetLockOwnershipCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	FencedLockGetLockOwnershipResponseFenceOffset     = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	FencedLockGetLockOwnershipResponseLockCountOffset = FencedLockGetLockOwnershipResponseFenceOffset + proto.LongSizeInBytes
	FencedLockGetLockOwnershipResponseSessionIdOffset = FencedLockGetLockOwnershipResponseLockCountOffset + proto.IntSizeInBytes
	FencedLockGetLockOwnershipResponseThreadIdOffset  = FencedLockGetLockOwnershipResponseSessionIdOffset + proto.LongSizeInBytes
)

// Returns current lock ownership status of the given FencedLock instance.

func EncodeFencedLockGetLockOwnershipRequest(groupId proto.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, FencedLockGetLockOwnershipCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(FencedLockGetLockOwnershipCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func (fencedlockGetLockOwnershipCodec) DecodeResponse(clientMessage *proto.ClientMessage) (fence int64, lockCount int32, sessionId int64, threadId int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	fence = FixSizedTypesCodec.DecodeLong(initialFrame.Content, FencedLockGetLockOwnershipResponseFenceOffset)
	lockCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, FencedLockGetLockOwnershipResponseLockCountOffset)
	sessionId = FixSizedTypesCodec.DecodeLong(initialFrame.Content, FencedLockGetLockOwnershipResponseSessionIdOffset)
	threadId = FixSizedTypesCodec.DecodeLong(initialFrame.Content, FencedLockGetLockOwnershipResponseThreadIdOffset)

	return fence, lockCount, sessionId, threadId
}
