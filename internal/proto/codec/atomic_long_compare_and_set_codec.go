/*
* Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License")
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package codec

import (
	"github.com/hazelcast/hazelcast-go-client/cp"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	AtomicLongCompareAndSetCodecRequestMessageType  = int32(0x090400)
	AtomicLongCompareAndSetCodecResponseMessageType = int32(0x090401)

	AtomicLongCompareAndSetCodecRequestExpectedOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicLongCompareAndSetCodecRequestUpdatedOffset    = AtomicLongCompareAndSetCodecRequestExpectedOffset + proto.LongSizeInBytes
	AtomicLongCompareAndSetCodecRequestInitialFrameSize = AtomicLongCompareAndSetCodecRequestUpdatedOffset + proto.LongSizeInBytes

	AtomicLongCompareAndSetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Atomically sets the value to the given updated value only if the current
// value the expected value.

func EncodeAtomicLongCompareAndSetRequest(groupId cp.RaftGroupId, name string, expected int64, updated int64) *proto.ClientMessage {
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
