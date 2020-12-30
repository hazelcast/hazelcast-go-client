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
	// hex: 0x0D0F00
	ReplicatedMapKeySetCodecRequestMessageType = int32(855808)
	// hex: 0x0D0F01
	ReplicatedMapKeySetCodecResponseMessageType = int32(855809)

	ReplicatedMapKeySetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Returns a lazy Set view of the key contained in this map. A LazySet is optimized for querying speed
// (preventing eager deserialization and hashing on HashSet insertion) and does NOT provide all operations.
// Any kind of mutating function will throw an UNSUPPORTED_OPERATION. Same is true for operations
// like java.util.Set#contains(Object) and java.util.Set#containsAll(java.util.Collection) which would result in
// very poor performance if called repeatedly (for example, in a loop). If the use case is different from querying
// the data, please copy the resulting set into a new java.util.HashSet.
type replicatedmapKeySetCodec struct{}

var ReplicatedMapKeySetCodec replicatedmapKeySetCodec

func (replicatedmapKeySetCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, ReplicatedMapKeySetCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapKeySetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (replicatedmapKeySetCodec) DecodeResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.ListMultiFrameCodec.DecodeForData(frameIterator)
}
