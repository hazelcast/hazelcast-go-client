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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x020F00
	MultiMapRemoveEntryListenerCodecRequestMessageType = int32(134912)
	// hex: 0x020F01
	MultiMapRemoveEntryListenerCodecResponseMessageType = int32(134913)

	MultiMapRemoveEntryListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapRemoveEntryListenerCodecRequestInitialFrameSize     = MultiMapRemoveEntryListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	MultiMapRemoveEntryListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified entry listener. If there is no such listener added before, this call does no change in the
// cluster and returns false.

func EncodeMultiMapRemoveEntryListenerRequest(name string, registrationId core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MultiMapRemoveEntryListenerCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MultiMapRemoveEntryListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapRemoveEntryListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeMultiMapRemoveEntryListenerResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MultiMapRemoveEntryListenerResponseResponseOffset)
}
