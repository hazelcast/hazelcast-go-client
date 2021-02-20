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

	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x170900
	RingbufferReadManyCodecRequestMessageType = int32(1509632)
	// hex: 0x170901
	RingbufferReadManyCodecResponseMessageType = int32(1509633)

	RingbufferReadManyCodecRequestStartSequenceOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	RingbufferReadManyCodecRequestMinCountOffset      = RingbufferReadManyCodecRequestStartSequenceOffset + proto.LongSizeInBytes
	RingbufferReadManyCodecRequestMaxCountOffset      = RingbufferReadManyCodecRequestMinCountOffset + proto.IntSizeInBytes
	RingbufferReadManyCodecRequestInitialFrameSize    = RingbufferReadManyCodecRequestMaxCountOffset + proto.IntSizeInBytes

	RingbufferReadManyResponseReadCountOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	RingbufferReadManyResponseNextSeqOffset   = RingbufferReadManyResponseReadCountOffset + proto.IntSizeInBytes
)

// Reads a batch of items from the Ringbuffer. If the number of available items after the first read item is smaller
// than the maxCount, these items are returned. So it could be the number of items read is smaller than the maxCount.
// If there are less items available than minCount, then this call blacks. Reading a batch of items is likely to
// perform better because less overhead is involved. A filter can be provided to only select items that need to be read.
// If the filter is null, all items are read. If the filter is not null, only items where the filter function returns
// true are returned. Using filters is a good way to prevent getting items that are of no value to the receiver.
// This reduces the amount of IO and the number of operations being executed, and can result in a significant performance improvement.
type ringbufferReadManyCodec struct{}

var RingbufferReadManyCodec ringbufferReadManyCodec

func (ringbufferReadManyCodec) EncodeRequest(name string, startSequence int64, minCount int32, maxCount int32, filter serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, RingbufferReadManyCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RingbufferReadManyCodecRequestStartSequenceOffset, startSequence)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, RingbufferReadManyCodecRequestMinCountOffset, minCount)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, RingbufferReadManyCodecRequestMaxCountOffset, maxCount)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingbufferReadManyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	CodecUtil.EncodeNullable(clientMessage, filter, DataCodec.Encode)

	return clientMessage
}

func (ringbufferReadManyCodec) DecodeResponse(clientMessage *proto.ClientMessage) (readCount int32, items []serialization.Data, itemSeqs []int64, nextSeq int64) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	readCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, RingbufferReadManyResponseReadCountOffset)
	nextSeq = FixSizedTypesCodec.DecodeLong(initialFrame.Content, RingbufferReadManyResponseNextSeqOffset)
	items = ListMultiFrameCodec.DecodeForData(frameIterator)
	itemSeqs = CodecUtil.DecodeNullableForLongArray(frameIterator)

	return readCount, items, itemSeqs, nextSeq
}
