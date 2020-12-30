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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x011400
	MapAddInterceptorCodecRequestMessageType = int32(70656)
	// hex: 0x011401
	MapAddInterceptorCodecResponseMessageType = int32(70657)

	MapAddInterceptorCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Adds an interceptor for this map. Added interceptor will intercept operations
// and execute user defined methods and will cancel operations if user defined method throw exception.
type mapAddInterceptorCodec struct{}

var MapAddInterceptorCodec mapAddInterceptorCodec

func (mapAddInterceptorCodec) EncodeRequest(name string, interceptor serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapAddInterceptorCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddInterceptorCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, interceptor)

	return clientMessage
}

func (mapAddInterceptorCodec) DecodeResponse(clientMessage *proto.ClientMessage) string {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.StringCodec.Decode(frameIterator)
}
