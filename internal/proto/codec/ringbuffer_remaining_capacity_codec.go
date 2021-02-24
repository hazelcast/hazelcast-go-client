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
	// hex: 0x170500
	RingbufferRemainingCapacityCodecRequestMessageType = int32(1508608)
	// hex: 0x170501
	RingbufferRemainingCapacityCodecResponseMessageType = int32(1508609)

	RingbufferRemainingCapacityCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	RingbufferRemainingCapacityResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the remaining capacity of the ringbuffer. The returned value could be stale as soon as it is returned.
// If ttl is not set, the remaining capacity will always be the capacity.

func EncodeRingbufferRemainingCapacityRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, RingbufferRemainingCapacityCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingbufferRemainingCapacityCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeRingbufferRemainingCapacityResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, RingbufferRemainingCapacityResponseResponseOffset)
}
