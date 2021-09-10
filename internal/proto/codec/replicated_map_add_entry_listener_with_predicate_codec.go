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
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x0D0B00
	ReplicatedMapAddEntryListenerWithPredicateCodecRequestMessageType = int32(854784)
	// hex: 0x0D0B01
	ReplicatedMapAddEntryListenerWithPredicateCodecResponseMessageType = int32(854785)

	// hex: 0x0D0B02
	ReplicatedMapAddEntryListenerWithPredicateCodecEventEntryMessageType = int32(854786)

	ReplicatedMapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerWithPredicateCodecRequestInitialFrameSize = ReplicatedMapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	ReplicatedMapAddEntryListenerWithPredicateResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	ReplicatedMapAddEntryListenerWithPredicateEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerWithPredicateEventEntryUuidOffset                    = ReplicatedMapAddEntryListenerWithPredicateEventEntryEventTypeOffset + proto.IntSizeInBytes
	ReplicatedMapAddEntryListenerWithPredicateEventEntryNumberOfAffectedEntriesOffset = ReplicatedMapAddEntryListenerWithPredicateEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds an continuous entry listener for this map. The listener will be notified for map add/remove/update/evict
// events filtered by the given predicate.

func EncodeReplicatedMapAddEntryListenerWithPredicateRequest(name string, predicate serialization.Data, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ReplicatedMapAddEntryListenerWithPredicateCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, ReplicatedMapAddEntryListenerWithPredicateCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapAddEntryListenerWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, predicate)

	return clientMessage
}

func DecodeReplicatedMapAddEntryListenerWithPredicateResponse(clientMessage *proto.ClientMessage) types.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerWithPredicateResponseResponseOffset)
}

func HandleReplicatedMapAddEntryListenerWithPredicate(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid types.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.Type()
	frameIterator := clientMessage.FrameIterator()
	if messageType == ReplicatedMapAddEntryListenerWithPredicateCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerWithPredicateEventEntryEventTypeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, ReplicatedMapAddEntryListenerWithPredicateEventEntryUuidOffset)
		numberOfAffectedEntries := FixSizedTypesCodec.DecodeInt(initialFrame.Content, ReplicatedMapAddEntryListenerWithPredicateEventEntryNumberOfAffectedEntriesOffset)
		key := DecodeNullableForData(frameIterator)
		value := DecodeNullableForData(frameIterator)
		oldValue := DecodeNullableForData(frameIterator)
		mergingValue := DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
