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
	// hex: 0x000E00
	ClientCreateProxiesCodecRequestMessageType = int32(3584)
	// hex: 0x000E01
	ClientCreateProxiesCodecResponseMessageType = int32(3585)

	ClientCreateProxiesCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Proxies will be created on all cluster members.
// If the member is  a lite member, a replicated map will not be created.
// Any proxy creation failure is logged on the server side.
// Exceptions related to a proxy creation failure is not send to the client.
// A proxy creation failure does not cancel this operation, all proxies will be attempted to be created.

func EncodeClientCreateProxiesRequest(proxies []proto.Pair) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ClientCreateProxiesCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientCreateProxiesCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeEntryListForStringAndString(clientMessage, proxies)

	return clientMessage
}
