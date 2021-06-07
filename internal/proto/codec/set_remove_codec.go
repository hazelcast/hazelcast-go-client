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
	// hex: 0x060500
	SetRemoveCodecRequestMessageType = int32(394496)
	// hex: 0x060501
	SetRemoveCodecResponseMessageType = int32(394497)

	SetRemoveCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SetRemoveResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified element from this set if it is present (optional operation).
// Returns true if this set contained the element (or equivalently, if this set changed as a result of the call).
// (This set will not contain the element once the call returns.)

func EncodeSetRemoveRequest(name string, value *serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SetRemoveCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetRemoveCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeSetRemoveResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SetRemoveResponseResponseOffset)
}
