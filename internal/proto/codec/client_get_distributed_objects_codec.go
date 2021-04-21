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
	// hex: 0x000800
	ClientGetDistributedObjectsCodecRequestMessageType = int32(2048)
	// hex: 0x000801
	ClientGetDistributedObjectsCodecResponseMessageType = int32(2049)

	ClientGetDistributedObjectsCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Gets the list of distributed objects in the cluster.
type clientGetDistributedObjectsCodec struct{}

var ClientGetDistributedObjectsCodec clientGetDistributedObjectsCodec

func (clientGetDistributedObjectsCodec) EncodeRequest() *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ClientGetDistributedObjectsCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientGetDistributedObjectsCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func (clientGetDistributedObjectsCodec) DecodeResponse(clientMessage *proto.ClientMessage) []proto.DistributedObjectInfo {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return ListMultiFrameCodec.DecodeForDistributedObjectInfo(frameIterator)
}
