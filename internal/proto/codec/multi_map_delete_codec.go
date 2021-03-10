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
	// hex: 0x021600
	MultiMapDeleteCodecRequestMessageType = int32(136704)
	// hex: 0x021601
	MultiMapDeleteCodecResponseMessageType = int32(136705)

	MultiMapDeleteCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapDeleteCodecRequestInitialFrameSize = MultiMapDeleteCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Removes all the entries with the given key.

func EncodeMultiMapDeleteRequest(name string, key serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MultiMapDeleteCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapDeleteCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapDeleteCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
