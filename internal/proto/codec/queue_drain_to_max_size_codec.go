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
	// hex: 0x030A00
	QueueDrainToMaxSizeCodecRequestMessageType = int32(199168)
	// hex: 0x030A01
	QueueDrainToMaxSizeCodecResponseMessageType = int32(199169)

	QueueDrainToMaxSizeCodecRequestMaxSizeOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueueDrainToMaxSizeCodecRequestInitialFrameSize = QueueDrainToMaxSizeCodecRequestMaxSizeOffset + proto.IntSizeInBytes
)

// Removes at most the given number of available elements from this queue and adds them to the given collection.
// A failure encountered while attempting to add elements to collection may result in elements being in neither,
// either or both collections when the associated exception is thrown. Attempts to drain a queue to itself result in
// ILLEGAL_ARGUMENT. Further, the behavior of this operation is undefined if the specified collection is
// modified while the operation is in progress.

func EncodeQueueDrainToMaxSizeRequest(name string, maxSize int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, QueueDrainToMaxSizeCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, QueueDrainToMaxSizeCodecRequestMaxSizeOffset, maxSize)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueDrainToMaxSizeCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeQueueDrainToMaxSizeResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeListMultiFrameForData(frameIterator)
}
