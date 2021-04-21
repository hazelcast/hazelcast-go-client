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
	// hex: 0x020300
	MultiMapRemoveCodecRequestMessageType = int32(131840)
	// hex: 0x020301
	MultiMapRemoveCodecResponseMessageType = int32(131841)

	MultiMapRemoveCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapRemoveCodecRequestInitialFrameSize = MultiMapRemoveCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Removes the given key value pair from the multimap.
type multimapRemoveCodec struct{}

var MultiMapRemoveCodec multimapRemoveCodec

func (multimapRemoveCodec) EncodeRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MultiMapRemoveCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapRemoveCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapRemoveCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (multimapRemoveCodec) DecodeResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.ListMultiFrameCodec.DecodeForData(frameIterator)
}
