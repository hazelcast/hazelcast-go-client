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
)

const (
	// hex: 0x011500
	MapRemoveInterceptorCodecRequestMessageType = int32(70912)
	// hex: 0x011501
	MapRemoveInterceptorCodecResponseMessageType = int32(70913)

	MapRemoveInterceptorCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	MapRemoveInterceptorResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the given interceptor for this map so it will not intercept operations anymore.

func EncodeMapRemoveInterceptorRequest(name string, id string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapRemoveInterceptorCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapRemoveInterceptorCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeString(clientMessage, id)

	return clientMessage
}

func DecodeMapRemoveInterceptorResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapRemoveInterceptorResponseResponseOffset)
}
