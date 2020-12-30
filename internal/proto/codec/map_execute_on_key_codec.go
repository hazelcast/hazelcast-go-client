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
	// hex: 0x012E00
	MapExecuteOnKeyCodecRequestMessageType = int32(77312)
	// hex: 0x012E01
	MapExecuteOnKeyCodecResponseMessageType = int32(77313)

	MapExecuteOnKeyCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapExecuteOnKeyCodecRequestInitialFrameSize = MapExecuteOnKeyCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Applies the user defined EntryProcessor to the entry mapped by the key. Returns the the object which is result of
// the process() method of EntryProcessor.
type mapExecuteOnKeyCodec struct{}

var MapExecuteOnKeyCodec mapExecuteOnKeyCodec

func (mapExecuteOnKeyCodec) EncodeRequest(name string, entryProcessor serialization.Data, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapExecuteOnKeyCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapExecuteOnKeyCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapExecuteOnKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, entryProcessor)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (mapExecuteOnKeyCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}
