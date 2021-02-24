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
	// hex: 0x013E00
	MapRemoveAllCodecRequestMessageType = int32(81408)
	// hex: 0x013E01
	MapRemoveAllCodecResponseMessageType = int32(81409)

	MapRemoveAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Removes all entries which match with the supplied predicate

func EncodeMapRemoveAllRequest(name string, predicate serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapRemoveAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapRemoveAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, predicate)

	return clientMessage
}
