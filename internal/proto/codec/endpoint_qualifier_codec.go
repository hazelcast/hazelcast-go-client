/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package codec

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	EndpointQualifierCodecTypeFieldOffset      = 0
	EndpointQualifierCodecTypeInitialFrameSize = EndpointQualifierCodecTypeFieldOffset + proto.IntSizeInBytes
)

/*
type endpointqualifierCodec struct {}

var EndpointQualifierCodec endpointqualifierCodec
*/

func EncodeEndpointQualifier(clientMessage *proto.ClientMessage, endpointQualifier pubcluster.EndpointQualifier) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, EndpointQualifierCodecTypeInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, EndpointQualifierCodecTypeFieldOffset, int32(endpointQualifier.Type))
	clientMessage.AddFrame(initialFrame)

	EncodeNullableForString(clientMessage, endpointQualifier.Identifier)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeEndpointQualifier(frameIterator *proto.ForwardFrameIterator) pubcluster.EndpointQualifier {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	_type := FixSizedTypesCodec.DecodeInt(initialFrame.Content, EndpointQualifierCodecTypeFieldOffset)

	identifier := DecodeNullableForString(frameIterator)
	FastForwardToEndFrame(frameIterator)
	return NewEndpointQualifier(_type, identifier)
}
