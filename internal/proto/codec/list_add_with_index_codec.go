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
	// hex: 0x051100
	ListAddWithIndexCodecRequestMessageType = int32(332032)
	// hex: 0x051101
	ListAddWithIndexCodecResponseMessageType = int32(332033)

	ListAddWithIndexCodecRequestIndexOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListAddWithIndexCodecRequestInitialFrameSize = ListAddWithIndexCodecRequestIndexOffset + proto.IntSizeInBytes
)

// Inserts the specified element at the specified position in this list (optional operation). Shifts the element
// currently at that position (if any) and any subsequent elements to the right (adds one to their indices).

func EncodeListAddWithIndexRequest(name string, index int32, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ListAddWithIndexCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListAddWithIndexCodecRequestIndexOffset, index)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListAddWithIndexCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, value)

	return clientMessage
}
