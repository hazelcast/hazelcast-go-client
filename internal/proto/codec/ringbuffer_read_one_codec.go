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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	RingbufferReadOneCodecRequestMessageType  = int32(0x170700)
	RingbufferReadOneCodecResponseMessageType = int32(0x170701)

	RingbufferReadOneCodecRequestSequenceOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	RingbufferReadOneCodecRequestInitialFrameSize = RingbufferReadOneCodecRequestSequenceOffset + proto.LongSizeInBytes
)

// EncodeRingbufferReadOneRequest
// Reads one item from the Ringbuffer. If the sequence is one beyond the current tail,
// this call blocks until an item is added. This method is not destructive unlike e.g. a queue.take.
// So the same item can be read by multiple readers, or it can be read multiple times
// by the same reader.
func EncodeRingbufferReadOneRequest(name string, sequence int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, RingbufferReadOneCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RingbufferReadOneCodecRequestSequenceOffset, sequence)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingbufferReadOneCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeRingbufferReadOneResponse(clientMessage *proto.ClientMessage) iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()
	return CodecUtil.DecodeNullableForData(frameIterator)
}
