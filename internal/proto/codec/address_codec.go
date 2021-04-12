/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	AddressCodecPortFieldOffset      = 0
	AddressCodecPortInitialFrameSize = AddressCodecPortFieldOffset + proto.IntSizeInBytes
)

/*
type addressCodec struct {}

var AddressCodec addressCodec
*/

func EncodeAddress(clientMessage *proto.ClientMessage, address cluster.Address) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, AddressCodecPortInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, AddressCodecPortFieldOffset, int32(address.Port()))
	clientMessage.AddFrame(initialFrame)

	EncodeString(clientMessage, address.Host())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeAddress(frameIterator *proto.ForwardFrameIterator) cluster.Address {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	port := FixSizedTypesCodec.DecodeInt(initialFrame.Content, AddressCodecPortFieldOffset)

	host := DecodeString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return cluster.NewAddress(host, port)
}
