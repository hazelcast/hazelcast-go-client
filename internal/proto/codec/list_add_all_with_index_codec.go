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
	// hex: 0x050E00
	ListAddAllWithIndexCodecRequestMessageType = int32(331264)
	// hex: 0x050E01
	ListAddAllWithIndexCodecResponseMessageType = int32(331265)

	ListAddAllWithIndexCodecRequestIndexOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListAddAllWithIndexCodecRequestInitialFrameSize = ListAddAllWithIndexCodecRequestIndexOffset + proto.IntSizeInBytes

	ListAddAllWithIndexResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Inserts all of the elements in the specified collection into this list at the specified position (optional operation).
// Shifts the element currently at that position (if any) and any subsequent elements to the right (increases their indices).
// The new elements will appear in this list in the order that they are returned by the specified collection's iterator.
// The behavior of this operation is undefined if the specified collection is modified while the operation is in progress.
// (Note that this will occur if the specified collection is this list, and it's nonempty.)

func EncodeListAddAllWithIndexRequest(name string, index int32, valueList []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ListAddAllWithIndexCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListAddAllWithIndexCodecRequestIndexOffset, index)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListAddAllWithIndexCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeListMultiFrameForData(clientMessage, valueList)

	return clientMessage
}

func DecodeListAddAllWithIndexResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ListAddAllWithIndexResponseResponseOffset)
}
