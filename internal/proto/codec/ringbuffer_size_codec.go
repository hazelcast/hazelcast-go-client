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
	// hex: 0x170100
	RingbufferSizeCodecRequestMessageType = int32(1507584)
	// hex: 0x170101
	RingbufferSizeCodecResponseMessageType = int32(1507585)

	RingbufferSizeCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	RingbufferSizeResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns number of items in the ringbuffer. If no ttl is set, the size will always be equal to capacity after the
// head completed the first looparound the ring. This is because no items are getting retired.

func EncodeRingbufferSizeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, RingbufferSizeCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingbufferSizeCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeRingbufferSizeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, RingbufferSizeResponseResponseOffset)
}
