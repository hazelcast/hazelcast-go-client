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
)

const (
	// hex: 0x1E0100
	CPGroupCreateCPGroupCodecRequestMessageType = int32(1966336)
	// hex: 0x1E0101
	CPGroupCreateCPGroupCodecResponseMessageType = int32(1966337)

	CPGroupCreateCPGroupCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Creates a new CP group with the given name

func EncodeCPGroupCreateCPGroupRequest(proxyName string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, CPGroupCreateCPGroupCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(CPGroupCreateCPGroupCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, proxyName)

	return clientMessage
}

func DecodeCPGroupCreateCPGroupResponse(clientMessage *proto.ClientMessage) proto.RaftGroupId {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeRaftGroupId(frameIterator)
}
