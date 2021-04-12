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
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	// hex: 0x001200
	ClientRemoveMigrationListenerCodecRequestMessageType = int32(4608)
	// hex: 0x001201
	ClientRemoveMigrationListenerCodecResponseMessageType = int32(4609)

	ClientRemoveMigrationListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientRemoveMigrationListenerCodecRequestInitialFrameSize     = ClientRemoveMigrationListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	ClientRemoveMigrationListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified migration listener.

func EncodeClientRemoveMigrationListenerRequest(registrationId internal.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientRemoveMigrationListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientRemoveMigrationListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientRemoveMigrationListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeClientRemoveMigrationListenerResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientRemoveMigrationListenerResponseResponseOffset)
}
