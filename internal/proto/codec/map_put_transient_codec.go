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
	// hex: 0x010D00
	MapPutTransientCodecRequestMessageType = int32(68864)
	// hex: 0x010D01
	MapPutTransientCodecResponseMessageType = int32(68865)

	MapPutTransientCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutTransientCodecRequestTtlOffset        = MapPutTransientCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapPutTransientCodecRequestInitialFrameSize = MapPutTransientCodecRequestTtlOffset + proto.LongSizeInBytes
)

// Same as put except that MapStore, if defined, will not be called to store/persist the entry.
// If ttl is 0, then the entry lives forever.
type mapPutTransientCodec struct{}

var MapPutTransientCodec mapPutTransientCodec

func (mapPutTransientCodec) EncodeRequest(name string, key serialization.Data, value serialization.Data, threadId int64, ttl int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapPutTransientCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutTransientCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapPutTransientCodecRequestTtlOffset, ttl)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutTransientCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, key)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}
