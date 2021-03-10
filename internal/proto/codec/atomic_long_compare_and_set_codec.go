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
	// hex: 0x090400
	AtomicLongCompareAndSetCodecRequestMessageType = int32(590848)
	// hex: 0x090401
	AtomicLongCompareAndSetCodecResponseMessageType = int32(590849)

	AtomicLongCompareAndSetCodecRequestExpectedOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicLongCompareAndSetCodecRequestUpdatedOffset    = AtomicLongCompareAndSetCodecRequestExpectedOffset + proto.LongSizeInBytes
	AtomicLongCompareAndSetCodecRequestInitialFrameSize = AtomicLongCompareAndSetCodecRequestUpdatedOffset + proto.LongSizeInBytes

	AtomicLongCompareAndSetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Atomically sets the value to the given updated value only if the current
// value the expected value.

func EncodeAtomicLongCompareAndSetRequest(groupId proto.RaftGroupId, name string, expected int64, updated int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, AtomicLongCompareAndSetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, AtomicLongCompareAndSetCodecRequestExpectedOffset, expected)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, AtomicLongCompareAndSetCodecRequestUpdatedOffset, updated)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongCompareAndSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeAtomicLongCompareAndSetResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, AtomicLongCompareAndSetResponseResponseOffset)
}
