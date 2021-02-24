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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x0A0200
	AtomicRefCompareAndSetCodecRequestMessageType = int32(655872)
	// hex: 0x0A0201
	AtomicRefCompareAndSetCodecResponseMessageType = int32(655873)

	AtomicRefCompareAndSetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	AtomicRefCompareAndSetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Alters the currently stored value by applying a function on it.

func EncodeAtomicRefCompareAndSetRequest(groupId proto.RaftGroupId, name string, oldValue serialization.Data, newValue serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, AtomicRefCompareAndSetCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefCompareAndSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	CodecUtil.EncodeNullable(clientMessage, oldValue, EncodeData)
	CodecUtil.EncodeNullable(clientMessage, newValue, EncodeData)

	return clientMessage
}

func DecodeAtomicRefCompareAndSetResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, AtomicRefCompareAndSetResponseResponseOffset)
}
