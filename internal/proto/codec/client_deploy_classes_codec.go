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
	// hex: 0x000D00
	ClientDeployClassesCodecRequestMessageType = int32(3328)
	// hex: 0x000D01
	ClientDeployClassesCodecResponseMessageType = int32(3329)

	ClientDeployClassesCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Deploys the list of classes to cluster
// Each item is a Map.Entry<String, byte[]> in the list.
// key of entry is full class name, and byte[] is the class definition.

func EncodeClientDeployClassesRequest(classDefinitions []proto.Pair) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, ClientDeployClassesCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientDeployClassesCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeEntryListForStringAndByteArray(clientMessage, classDefinitions)

	return clientMessage
}
