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
	// hex: 0x060600
	SetAddAllCodecRequestMessageType = int32(394752)
	// hex: 0x060601
	SetAddAllCodecResponseMessageType = int32(394753)

	SetAddAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	SetAddAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Adds all of the elements in the specified collection to this set if they're not already present
// (optional operation). If the specified collection is also a set, the addAll operation effectively modifies this
// set so that its value is the union of the two sets. The behavior of this operation is undefined if the specified
// collection is modified while the operation is in progress.
type setAddAllCodec struct{}

var SetAddAllCodec setAddAllCodec

func (setAddAllCodec) EncodeRequest(name string, valueList []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, SetAddAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SetAddAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	ListMultiFrameCodec.EncodeForData(clientMessage, valueList)

	return clientMessage
}

func (setAddAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SetAddAllResponseResponseOffset)
}
