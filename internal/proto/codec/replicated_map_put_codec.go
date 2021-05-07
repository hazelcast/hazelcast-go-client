/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	// hex: 0x0D0100
	ReplicatedMapPutCodecRequestMessageType = int32(852224)
	// hex: 0x0D0101
	ReplicatedMapPutCodecResponseMessageType = int32(852225)

	ReplicatedMapPutCodecRequestTtlOffset        = proto.PartitionIDOffset + proto.IntSizeInBytes
	ReplicatedMapPutCodecRequestInitialFrameSize = ReplicatedMapPutCodecRequestTtlOffset + proto.LongSizeInBytes
)

// Associates a given value to the specified key and replicates it to the cluster. If there is an old value, it will
// be replaced by the specified one and returned from the call. In addition, you have to specify a ttl and its TimeUnit
// to define when the value is outdated and thus should be removed from the replicated map.

func EncodeReplicatedMapPutRequest(name string, key *iserialization.Data, value *iserialization.Data, ttl int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ReplicatedMapPutCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, ReplicatedMapPutCodecRequestTtlOffset, ttl)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapPutCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeReplicatedMapPutResponse(clientMessage *proto.ClientMessage) *iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return CodecUtil.DecodeNullableForData(frameIterator)
}
