/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	CPMapSetCodecRequestMessageType  = int32(0x230300)
	CPMapSetCodecResponseMessageType = int32(0x230301)

	CPMapSetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Sets the key-value in the specified map.

func EncodeCPMapSetRequest(groupId types.RaftGroupID, name string, key iserialization.Data, value iserialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, CPMapSetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPMapSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)
	EncodeData(clientMessage, value)

	return clientMessage
}

func DecodeCPMapSetResponse(clientMessage *proto.ClientMessage) iserialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeNullableForData(frameIterator)
}
