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
	// hex: 0x050600
	ListAddAllCodecRequestMessageType = int32(329216)
	// hex: 0x050601
	ListAddAllCodecResponseMessageType = int32(329217)

	ListAddAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ListAddAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Appends all of the elements in the specified collection to the end of this list, in the order that they are
// returned by the specified collection's iterator (optional operation).
// The behavior of this operation is undefined if the specified collection is modified while the operation is in progress.
// (Note that this will occur if the specified collection is this list, and it's nonempty.)
type listAddAllCodec struct{}

var ListAddAllCodec listAddAllCodec

func (listAddAllCodec) EncodeRequest(name string, valueList []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ListAddAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListAddAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.ListMultiFrameCodec.EncodeForData(clientMessage, valueList)

	return clientMessage
}

func (listAddAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ListAddAllResponseResponseOffset)
}
