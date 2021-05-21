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
	// hex: 0x031000
	QueueAddAllCodecRequestMessageType = int32(200704)
	// hex: 0x031001
	QueueAddAllCodecResponseMessageType = int32(200705)

	QueueAddAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	QueueAddAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Adds all of the elements in the specified collection to this collection (optional operation).The behavior of this
// operation is undefined if the specified collection is modified while the operation is in progress.
// (This implies that the behavior of this call is undefined if the specified collection is this collection,
// and this collection is nonempty.)

func EncodeQueueAddAllRequest(name string, dataList []*iserialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, QueueAddAllCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueAddAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeListMultiFrameForData(clientMessage, dataList)

	return clientMessage
}

func DecodeQueueAddAllResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, QueueAddAllResponseResponseOffset)
}
