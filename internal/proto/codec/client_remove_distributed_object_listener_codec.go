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
	// hex: 0x000A00
	ClientRemoveDistributedObjectListenerCodecRequestMessageType = int32(2560)
	// hex: 0x000A01
	ClientRemoveDistributedObjectListenerCodecResponseMessageType = int32(2561)

	ClientRemoveDistributedObjectListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientRemoveDistributedObjectListenerCodecRequestInitialFrameSize     = ClientRemoveDistributedObjectListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	ClientRemoveDistributedObjectListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified distributed object listener. If there is no such listener added before, this call does no
// change in the cluster and returns false.

func EncodeClientRemoveDistributedObjectListenerRequest(registrationId internal.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientRemoveDistributedObjectListenerCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, ClientRemoveDistributedObjectListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientRemoveDistributedObjectListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeClientRemoveDistributedObjectListenerResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ClientRemoveDistributedObjectListenerResponseResponseOffset)
}
