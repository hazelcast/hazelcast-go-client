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
	// hex: 0x090600
	AtomicLongGetAndAddCodecRequestMessageType = int32(591360)
	// hex: 0x090601
	AtomicLongGetAndAddCodecResponseMessageType = int32(591361)

	AtomicLongGetAndAddCodecRequestDeltaOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicLongGetAndAddCodecRequestInitialFrameSize = AtomicLongGetAndAddCodecRequestDeltaOffset + proto.LongSizeInBytes

	AtomicLongGetAndAddResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Atomically adds the given value to the current value.

func EncodeAtomicLongGetAndAddRequest(groupId proto.RaftGroupId, name string, delta int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, AtomicLongGetAndAddCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, AtomicLongGetAndAddCodecRequestDeltaOffset, delta)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongGetAndAddCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeAtomicLongGetAndAddResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, AtomicLongGetAndAddResponseResponseOffset)
}
