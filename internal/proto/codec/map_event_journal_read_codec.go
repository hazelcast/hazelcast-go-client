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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x014200
	MapEventJournalReadCodecRequestMessageType = int32(82432)
	// hex: 0x014201
	MapEventJournalReadCodecResponseMessageType = int32(82433)

	MapEventJournalReadCodecRequestStartSequenceOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapEventJournalReadCodecRequestMinSizeOffset       = MapEventJournalReadCodecRequestStartSequenceOffset + proto.LongSizeInBytes
	MapEventJournalReadCodecRequestMaxSizeOffset       = MapEventJournalReadCodecRequestMinSizeOffset + proto.IntSizeInBytes
	MapEventJournalReadCodecRequestInitialFrameSize    = MapEventJournalReadCodecRequestMaxSizeOffset + proto.IntSizeInBytes

	MapEventJournalReadResponseReadCountOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapEventJournalReadResponseNextSeqOffset   = MapEventJournalReadResponseReadCountOffset + proto.IntSizeInBytes
)

// Reads from the map event journal in batches. You may specify the start sequence,
// the minumum required number of items in the response, the maximum number of items
// in the response, a predicate that the events should pass and a projection to
// apply to the events in the journal.
// If the event journal currently contains less events than {@code minSize}, the
// call will wait until it has sufficient items.
// The predicate, filter and projection may be {@code null} in which case all elements are returned
// and no projection is applied.

func EncodeMapEventJournalReadRequest(name string, startSequence int64, minSize int32, maxSize int32, predicate serialization.Data, projection serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapEventJournalReadCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapEventJournalReadCodecRequestStartSequenceOffset, startSequence)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapEventJournalReadCodecRequestMinSizeOffset, minSize)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapEventJournalReadCodecRequestMaxSizeOffset, maxSize)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapEventJournalReadCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	CodecUtil.EncodeNullable(clientMessage, predicate, EncodeData)
	CodecUtil.EncodeNullable(clientMessage, projection, EncodeData)

	return clientMessage
}

func DecodeMapEventJournalReadResponse(clientMessage *proto.ClientMessage) (readCount int32, items []serialization.Data, itemSeqs []int64, nextSeq int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	readCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapEventJournalReadResponseReadCountOffset)
	nextSeq = FixSizedTypesCodec.DecodeLong(initialFrame.Content, MapEventJournalReadResponseNextSeqOffset)
	items = DecodeListMultiFrameForData(frameIterator)
	itemSeqs = CodecUtil.DecodeNullableForLongArray(frameIterator)

	return readCount, items, itemSeqs, nextSeq
}
