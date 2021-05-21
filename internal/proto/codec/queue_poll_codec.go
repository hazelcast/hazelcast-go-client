/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	// hex: 0x030500
	QueuePollCodecRequestMessageType = int32(197888)
	// hex: 0x030501
	QueuePollCodecResponseMessageType = int32(197889)

	QueuePollCodecRequestTimeoutMillisOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueuePollCodecRequestInitialFrameSize    = QueuePollCodecRequestTimeoutMillisOffset + proto.LongSizeInBytes
)

// Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element
// to become available.

func EncodeQueuePollRequest(name string, timeoutMillis int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, QueuePollCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, QueuePollCodecRequestTimeoutMillisOffset, timeoutMillis)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueuePollCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeQueuePollResponse(clientMessage *proto.ClientMessage) *iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
