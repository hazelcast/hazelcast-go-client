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
	// hex: 0x011F00
	MapEvictAllCodecRequestMessageType = int32(73472)
	// hex: 0x011F01
	MapEvictAllCodecResponseMessageType = int32(73473)

	MapEvictAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Evicts all keys from this map except the locked ones. If a MapStore is defined for this map, deleteAll is not
// called by this method. If you do want to deleteAll to be called use the clear method. The EVICT_ALL event is
// fired for any registered listeners.

func EncodeMapEvictAllRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapEvictAllCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapEvictAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}
