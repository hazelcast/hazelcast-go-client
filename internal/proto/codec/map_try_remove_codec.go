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
	// hex: 0x010B00
	MapTryRemoveCodecRequestMessageType = int32(68352)
	// hex: 0x010B01
	MapTryRemoveCodecResponseMessageType = int32(68353)

	MapTryRemoveCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapTryRemoveCodecRequestTimeoutOffset    = MapTryRemoveCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapTryRemoveCodecRequestInitialFrameSize = MapTryRemoveCodecRequestTimeoutOffset + proto.LongSizeInBytes

	MapTryRemoveResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Tries to remove the entry with the given key from this map within the specified timeout value.
// If the key is already locked by another thread and/or member, then this operation will wait the timeout
// amount for acquiring the lock.

func EncodeMapTryRemoveRequest(name string, key *iserialization.Data, threadId int64, timeout int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapTryRemoveCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryRemoveCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryRemoveCodecRequestTimeoutOffset, timeout)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapTryRemoveCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapTryRemoveResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapTryRemoveResponseResponseOffset)
}
