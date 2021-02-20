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
	// hex: 0x060700
	SetCompareAndRemoveAllCodecRequestMessageType = int32(395008)
	// hex: 0x060701
	SetCompareAndRemoveAllCodecResponseMessageType = int32(395009)

	SetCompareAndRemoveAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SetCompareAndRemoveAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes from this set all of its elements that are contained in the specified collection (optional operation).
// If the specified collection is also a set, this operation effectively modifies this set so that its value is the
// asymmetric set difference of the two sets.
type setCompareAndRemoveAllCodec struct{}

var SetCompareAndRemoveAllCodec setCompareAndRemoveAllCodec

func (setCompareAndRemoveAllCodec) EncodeRequest(name string, values []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, SetCompareAndRemoveAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetCompareAndRemoveAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	ListMultiFrameCodec.EncodeForData(clientMessage, values)

	return clientMessage
}

func (setCompareAndRemoveAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SetCompareAndRemoveAllResponseResponseOffset)
}
