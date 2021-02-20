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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/config"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	BitmapIndexOptionsCodecUniqueKeyTransformationFieldOffset      = 0
	BitmapIndexOptionsCodecUniqueKeyTransformationInitialFrameSize = BitmapIndexOptionsCodecUniqueKeyTransformationFieldOffset + proto.IntSizeInBytes
)

type bitmapindexoptionsCodec struct{}

var BitmapIndexOptionsCodec bitmapindexoptionsCodec

func (bitmapindexoptionsCodec) Encode(clientMessage *proto.ClientMessage, bitmapIndexOptions config.BitmapIndexOptions) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, BitmapIndexOptionsCodecUniqueKeyTransformationInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, BitmapIndexOptionsCodecUniqueKeyTransformationFieldOffset, bitmapIndexOptions.GetUniqueKeyTransformation())
	clientMessage.AddFrame(initialFrame)

	StringCodec.Encode(clientMessage, bitmapIndexOptions.GetUniqueKey())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (bitmapindexoptionsCodec) Decode(frameIterator *proto.ForwardFrameIterator) config.BitmapIndexOptions {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	uniqueKeyTransformation := FixSizedTypesCodec.DecodeInt(initialFrame.Content, BitmapIndexOptionsCodecUniqueKeyTransformationFieldOffset)

	uniqueKey := StringCodec.Decode(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return config.NewBitmapIndexOptions(uniqueKey, uniqueKeyTransformation)
}
