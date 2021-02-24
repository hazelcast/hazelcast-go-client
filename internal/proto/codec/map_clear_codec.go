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
	// hex: 0x012D00
	MapClearCodecRequestMessageType = int32(77056)
	// hex: 0x012D01
	MapClearCodecResponseMessageType = int32(77057)

	MapClearCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// This method clears the map and invokes MapStore#deleteAll deleteAll on MapStore which, if connected to a database,
// will delete the records from that database. The MAP_CLEARED event is fired for any registered listeners.
// To clear a map without calling MapStore#deleteAll, use #evictAll.

func EncodeMapClearRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapClearCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapClearCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}
