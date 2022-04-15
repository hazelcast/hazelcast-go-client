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
	// hex: 0x020900
	MultiMapContainsEntryCodecRequestMessageType = int32(133376)
	// hex: 0x020901
	MultiMapContainsEntryCodecResponseMessageType = int32(133377)

	MultiMapContainsEntryCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapContainsEntryCodecRequestInitialFrameSize = MultiMapContainsEntryCodecRequestThreadIdOffset + proto.LongSizeInBytes

	MultiMapContainsEntryResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns whether the multimap contains the given key-value pair.

func EncodeMultiMapContainsEntryRequest(name string, key serialization.Data, value serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MultiMapContainsEntryCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapContainsEntryCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapContainsEntryCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeMultiMapContainsEntryResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MultiMapContainsEntryResponseResponseOffset)
}
