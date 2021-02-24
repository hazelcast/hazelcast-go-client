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
	// hex: 0x0A0400
	AtomicRefGetCodecRequestMessageType = int32(656384)
	// hex: 0x0A0401
	AtomicRefGetCodecResponseMessageType = int32(656385)

	AtomicRefGetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Gets the current value.

func EncodeAtomicRefGetRequest(groupId proto.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, AtomicRefGetCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefGetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeAtomicRefGetResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
