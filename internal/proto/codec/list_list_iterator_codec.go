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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x051700
	ListListIteratorCodecRequestMessageType = int32(333568)
	// hex: 0x051701
	ListListIteratorCodecResponseMessageType = int32(333569)

	ListListIteratorCodecRequestIndexOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListListIteratorCodecRequestInitialFrameSize = ListListIteratorCodecRequestIndexOffset + proto.IntSizeInBytes
)

// Returns a list iterator over the elements in this list (in proper sequence), starting at the specified position
// in the list. The specified index indicates the first element that would be returned by an initial call to
// ListIterator#next next. An initial call to ListIterator#previous previous would return the element with the
// specified index minus one.

func EncodeListListIteratorRequest(name string, index int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ListListIteratorCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListListIteratorCodecRequestIndexOffset, index)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListListIteratorCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeListListIteratorResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeListMultiFrameForData(frameIterator)
}
