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
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x060C00
	SetRemoveListenerCodecRequestMessageType = int32(396288)
	// hex: 0x060C01
	SetRemoveListenerCodecResponseMessageType = int32(396289)

	SetRemoveListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	SetRemoveListenerCodecRequestInitialFrameSize     = SetRemoveListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	SetRemoveListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified item listener. If there is no such listener added before, this call does no change in the
// cluster and returns false.

func EncodeSetRemoveListenerRequest(name string, registrationId types.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, SetRemoveListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeUUID(initialFrame.Content, SetRemoveListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetRemoveListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeSetRemoveListenerResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeBoolean(initialFrame.Content, SetRemoveListenerResponseResponseOffset)
}
