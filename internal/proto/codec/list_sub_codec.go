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
	// hex: 0x051500
	ListSubCodecRequestMessageType = int32(333056)
	// hex: 0x051501
	ListSubCodecResponseMessageType = int32(333057)

	ListSubCodecRequestFromOffset       = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListSubCodecRequestToOffset         = ListSubCodecRequestFromOffset + proto.IntSizeInBytes
	ListSubCodecRequestInitialFrameSize = ListSubCodecRequestToOffset + proto.IntSizeInBytes
)

// Returns a view of the portion of this list between the specified from, inclusive, and to, exclusive.(If from and
// to are equal, the returned list is empty.) The returned list is backed by this list, so non-structural changes in
// the returned list are reflected in this list, and vice-versa. The returned list supports all of the optional list
// operations supported by this list.
// This method eliminates the need for explicit range operations (of the sort that commonly exist for arrays).
// Any operation that expects a list can be used as a range operation by passing a subList view instead of a whole list.
// Similar idioms may be constructed for indexOf and lastIndexOf, and all of the algorithms in the Collections class
// can be applied to a subList.
// The semantics of the list returned by this method become undefined if the backing list (i.e., this list) is
// structurally modified in any way other than via the returned list.(Structural modifications are those that change
// the size of this list, or otherwise perturb it in such a fashion that iterations in progress may yield incorrect results.)

func EncodeListSubRequest(name string, from int32, to int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ListSubCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListSubCodecRequestFromOffset, from)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListSubCodecRequestToOffset, to)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListSubCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeListSubResponse(clientMessage *proto.ClientMessage) []iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeListMultiFrameForData(frameIterator)
}
