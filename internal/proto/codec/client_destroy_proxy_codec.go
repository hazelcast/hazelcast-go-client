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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x000500
	ClientDestroyProxyCodecRequestMessageType = int32(1280)
	// hex: 0x000501
	ClientDestroyProxyCodecResponseMessageType = int32(1281)

	ClientDestroyProxyCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Destroys the proxy given by its name cluster-wide. Also, clears and releases all resources of this proxy.
type clientDestroyProxyCodec struct{}

var ClientDestroyProxyCodec clientDestroyProxyCodec

func (clientDestroyProxyCodec) EncodeRequest(name string, serviceName string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ClientDestroyProxyCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientDestroyProxyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.StringCodec.Encode(clientMessage, serviceName)

	return clientMessage
}
