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
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	// hex: 0x060700
	SetCompareAndRemoveAllCodecRequestMessageType = int32(395008)
	// hex: 0x060701
	SetCompareAndRemoveAllCodecResponseMessageType = int32(395009)

	SetCompareAndRemoveAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SetCompareAndRemoveAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes from this set all of its elements that are contained in the specified collection (optional operation).
// If the specified collection is also a set, this operation effectively modifies this set so that its value is the
// asymmetric set difference of the two sets.

func EncodeSetCompareAndRemoveAllRequest(name string, values []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SetCompareAndRemoveAllCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetCompareAndRemoveAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeListMultiFrameForData(clientMessage, values)

	return clientMessage
}

func DecodeSetCompareAndRemoveAllResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SetCompareAndRemoveAllResponseResponseOffset)
}
