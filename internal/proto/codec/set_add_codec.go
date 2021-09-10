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
	// hex: 0x060400
	SetAddCodecRequestMessageType = int32(394240)
	// hex: 0x060401
	SetAddCodecResponseMessageType = int32(394241)

	SetAddCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SetAddResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Adds the specified element to this set if it is not already present (optional operation).
// If this set already contains the element, the call leaves the set unchanged and returns false.In combination with
// the restriction on constructors, this ensures that sets never contain duplicate elements.
// The stipulation above does not imply that sets must accept all elements; sets may refuse to add any particular
// element, including null, and throw an exception, as described in the specification for Collection
// Individual set implementations should clearly document any restrictions on the elements that they may contain.

func EncodeSetAddRequest(name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SetAddCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetAddCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeSetAddResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeBoolean(initialFrame.Content, SetAddResponseResponseOffset)
}
