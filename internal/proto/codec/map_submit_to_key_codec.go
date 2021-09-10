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
	// hex: 0x012F00
	MapSubmitToKeyCodecRequestMessageType = int32(77568)
	// hex: 0x012F01
	MapSubmitToKeyCodecResponseMessageType = int32(77569)

	MapSubmitToKeyCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapSubmitToKeyCodecRequestInitialFrameSize = MapSubmitToKeyCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Applies the user defined EntryProcessor to the entry mapped by the key. Returns immediately with a Future
// representing that task.EntryProcessor is not cancellable, so calling Future.cancel() method won't cancel the
// operation of EntryProcessor.

func EncodeMapSubmitToKeyRequest(name string, entryProcessor serialization.Data, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapSubmitToKeyCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapSubmitToKeyCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapSubmitToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, entryProcessor)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapSubmitToKeyResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeNullableForData(frameIterator)
}
