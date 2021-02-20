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
	// hex: 0x012300
	MapGetAllCodecRequestMessageType = int32(74496)
	// hex: 0x012301
	MapGetAllCodecResponseMessageType = int32(74497)

	MapGetAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Returns the entries for the given keys. If any keys are not present in the Map, it will call loadAll The returned
// map is NOT backed by the original map, so changes to the original map are NOT reflected in the returned map, and vice-versa.
// Please note that all the keys in the request should belong to the partition id to which this request is being sent, all keys
// matching to a different partition id shall be ignored. The API implementation using this request may need to send multiple
// of these request messages for filling a request for a key set if the keys belong to different partitions.
type mapGetAllCodec struct{}

var MapGetAllCodec mapGetAllCodec

func (mapGetAllCodec) EncodeRequest(name string, keys []serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapGetAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapGetAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	ListMultiFrameCodec.EncodeForData(clientMessage, keys)

	return clientMessage
}

func (mapGetAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) []proto.Pair {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return EntryListCodec.DecodeForDataAndData(frameIterator)
}
