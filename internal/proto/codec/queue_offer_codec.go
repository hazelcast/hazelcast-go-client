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
	// hex: 0x030100
	QueueOfferCodecRequestMessageType = int32(196864)
	// hex: 0x030101
	QueueOfferCodecResponseMessageType = int32(196865)

	QueueOfferCodecRequestTimeoutMillisOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	QueueOfferCodecRequestInitialFrameSize    = QueueOfferCodecRequestTimeoutMillisOffset + proto.LongSizeInBytes

	QueueOfferResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Inserts the specified element into this queue, waiting up to the specified wait time if necessary for space to
// become available.

func EncodeQueueOfferRequest(name string, value serialization.Data, timeoutMillis int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueueOfferCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, QueueOfferCodecRequestTimeoutMillisOffset, timeoutMillis)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueOfferCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeQueueOfferResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, QueueOfferResponseResponseOffset)
}
