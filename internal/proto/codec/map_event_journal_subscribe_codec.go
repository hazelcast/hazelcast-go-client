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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x014100
	MapEventJournalSubscribeCodecRequestMessageType = int32(82176)
	// hex: 0x014101
	MapEventJournalSubscribeCodecResponseMessageType = int32(82177)

	MapEventJournalSubscribeCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	MapEventJournalSubscribeResponseOldestSequenceOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapEventJournalSubscribeResponseNewestSequenceOffset = MapEventJournalSubscribeResponseOldestSequenceOffset + proto.LongSizeInBytes
)

// Performs the initial subscription to the map event journal.
// This includes retrieving the event journal sequences of the
// oldest and newest event in the journal.
type mapEventJournalSubscribeCodec struct{}

var MapEventJournalSubscribeCodec mapEventJournalSubscribeCodec

func (mapEventJournalSubscribeCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapEventJournalSubscribeCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapEventJournalSubscribeCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (mapEventJournalSubscribeCodec) DecodeResponse(clientMessage *proto.ClientMessage) (oldestSequence int64, newestSequence int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	oldestSequence = FixSizedTypesCodec.DecodeLong(initialFrame.Content, MapEventJournalSubscribeResponseOldestSequenceOffset)
	newestSequence = FixSizedTypesCodec.DecodeLong(initialFrame.Content, MapEventJournalSubscribeResponseNewestSequenceOffset)

	return oldestSequence, newestSequence
}
