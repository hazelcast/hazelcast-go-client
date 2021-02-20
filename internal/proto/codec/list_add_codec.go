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
	// hex: 0x050400
	ListAddCodecRequestMessageType = int32(328704)
	// hex: 0x050401
	ListAddCodecResponseMessageType = int32(328705)

	ListAddCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ListAddResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Appends the specified element to the end of this list (optional operation). Lists that support this operation may
// place limitations on what elements may be added to this list.  In particular, some lists will refuse to add null
// elements, and others will impose restrictions on the type of elements that may be added. List classes should
// clearly specify in their documentation any restrictions on what elements may be added.
type listAddCodec struct{}

var ListAddCodec listAddCodec

func (listAddCodec) EncodeRequest(name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ListAddCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListAddCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (listAddCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ListAddResponseResponseOffset)
}
