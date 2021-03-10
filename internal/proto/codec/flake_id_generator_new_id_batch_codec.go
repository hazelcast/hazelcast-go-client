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
	// hex: 0x1C0100
	FlakeIdGeneratorNewIdBatchCodecRequestMessageType = int32(1835264)
	// hex: 0x1C0101
	FlakeIdGeneratorNewIdBatchCodecResponseMessageType = int32(1835265)

	FlakeIdGeneratorNewIdBatchCodecRequestBatchSizeOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	FlakeIdGeneratorNewIdBatchCodecRequestInitialFrameSize = FlakeIdGeneratorNewIdBatchCodecRequestBatchSizeOffset + proto.IntSizeInBytes

	FlakeIdGeneratorNewIdBatchResponseBaseOffset      = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	FlakeIdGeneratorNewIdBatchResponseIncrementOffset = FlakeIdGeneratorNewIdBatchResponseBaseOffset + proto.LongSizeInBytes
	FlakeIdGeneratorNewIdBatchResponseBatchSizeOffset = FlakeIdGeneratorNewIdBatchResponseIncrementOffset + proto.LongSizeInBytes
)

// Fetches a new batch of ids for the given flake id generator.

func EncodeFlakeIdGeneratorNewIdBatchRequest(name string, batchSize int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, FlakeIdGeneratorNewIdBatchCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, FlakeIdGeneratorNewIdBatchCodecRequestBatchSizeOffset, batchSize)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(FlakeIdGeneratorNewIdBatchCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}

func DecodeFlakeIdGeneratorNewIdBatchResponse(clientMessage *proto.ClientMessage) (base int64, increment int64, batchSize int32) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	base = FixSizedTypesCodec.DecodeLong(initialFrame.Content, FlakeIdGeneratorNewIdBatchResponseBaseOffset)
	increment = FixSizedTypesCodec.DecodeLong(initialFrame.Content, FlakeIdGeneratorNewIdBatchResponseIncrementOffset)
	batchSize = FixSizedTypesCodec.DecodeInt(initialFrame.Content, FlakeIdGeneratorNewIdBatchResponseBatchSizeOffset)

	return base, increment, batchSize
}
