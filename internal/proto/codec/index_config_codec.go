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
	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	IndexConfigCodecTypeFieldOffset      = 0
	IndexConfigCodecTypeInitialFrameSize = IndexConfigCodecTypeFieldOffset + proto.IntSizeInBytes
)

/*
type indexconfigCodec struct {}

var IndexConfigCodec indexconfigCodec
*/

func EncodeIndexConfig(clientMessage *proto.ClientMessage, indexConfig hztypes.IndexConfig) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, IndexConfigCodecTypeInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, IndexConfigCodecTypeFieldOffset, int32(indexConfig.Type()))
	clientMessage.AddFrame(initialFrame)

	CodecUtil.EncodeNullableForString(clientMessage, indexConfig.Name())
	EncodeListMultiFrameForString(clientMessage, indexConfig.Attributes())
	CodecUtil.EncodeNullableForBitmapIndexOptions(clientMessage, indexConfig.BitmapIndexOptions())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeIndexConfig(frameIterator *proto.ForwardFrameIterator) hztypes.IndexConfig {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	_type := FixSizedTypesCodec.DecodeInt(initialFrame.Content, IndexConfigCodecTypeFieldOffset)

	name := CodecUtil.DecodeNullableForString(frameIterator)
	attributes := DecodeListMultiFrameForString(frameIterator)
	bitmapIndexOptions := CodecUtil.DecodeNullableForBitmapIndexOptions(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return hztypes.NewIndexConfig(name, _type, attributes, bitmapIndexOptions)
}
