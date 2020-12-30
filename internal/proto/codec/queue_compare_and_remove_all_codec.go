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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x030D00
	QueueCompareAndRemoveAllCodecRequestMessageType = int32(199936)
	// hex: 0x030D01
	QueueCompareAndRemoveAllCodecResponseMessageType = int32(199937)

	QueueCompareAndRemoveAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	QueueCompareAndRemoveAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes all of this collection's elements that are also contained in the specified collection (optional operation).
// After this call returns, this collection will contain no elements in common with the specified collection.
type queueCompareAndRemoveAllCodec struct{}

var QueueCompareAndRemoveAllCodec queueCompareAndRemoveAllCodec

func (queueCompareAndRemoveAllCodec) EncodeRequest(name string, dataList []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueueCompareAndRemoveAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueueCompareAndRemoveAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.ListMultiFrameCodec.EncodeForData(clientMessage, dataList)

	return clientMessage
}

func (queueCompareAndRemoveAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, QueueCompareAndRemoveAllResponseResponseOffset)
}
