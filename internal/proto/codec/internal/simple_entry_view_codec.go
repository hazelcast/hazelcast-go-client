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
package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	SimpleEntryViewCodecCostFieldOffset           = 0
	SimpleEntryViewCodecCreationTimeFieldOffset   = SimpleEntryViewCodecCostFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecExpirationTimeFieldOffset = SimpleEntryViewCodecCreationTimeFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecHitsFieldOffset           = SimpleEntryViewCodecExpirationTimeFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecLastAccessTimeFieldOffset = SimpleEntryViewCodecHitsFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecLastStoredTimeFieldOffset = SimpleEntryViewCodecLastAccessTimeFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecLastUpdateTimeFieldOffset = SimpleEntryViewCodecLastStoredTimeFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecVersionFieldOffset        = SimpleEntryViewCodecLastUpdateTimeFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecTtlFieldOffset            = SimpleEntryViewCodecVersionFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecMaxIdleFieldOffset        = SimpleEntryViewCodecTtlFieldOffset + proto.LongSizeInBytes
	SimpleEntryViewCodecMaxIdleInitialFrameSize   = SimpleEntryViewCodecMaxIdleFieldOffset + proto.LongSizeInBytes
)

type simpleentryviewCodec struct{}

var SimpleEntryViewCodec simpleentryviewCodec

func (simpleentryviewCodec) Encode(clientMessage *proto.ClientMessage, simpleEntryView core.SimpleEntryView) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, SimpleEntryViewCodecMaxIdleInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecCostFieldOffset, simpleEntryView.GetCost())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecCreationTimeFieldOffset, simpleEntryView.GetCreationTime())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecExpirationTimeFieldOffset, simpleEntryView.GetExpirationTime())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecHitsFieldOffset, simpleEntryView.GetHits())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastAccessTimeFieldOffset, simpleEntryView.GetLastAccessTime())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastStoredTimeFieldOffset, simpleEntryView.GetLastStoredTime())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastUpdateTimeFieldOffset, simpleEntryView.GetLastUpdateTime())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecVersionFieldOffset, simpleEntryView.GetVersion())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecTtlFieldOffset, simpleEntryView.GetTtl())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SimpleEntryViewCodecMaxIdleFieldOffset, simpleEntryView.GetMaxIdle())
	clientMessage.AddFrame(initialFrame)

	DataCodec.Encode(clientMessage, simpleEntryView.GetKey())
	DataCodec.Encode(clientMessage, simpleEntryView.GetValue())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (simpleentryviewCodec) Decode(frameIterator *proto.ForwardFrameIterator) core.SimpleEntryView {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	cost := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecCostFieldOffset)
	creationTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecCreationTimeFieldOffset)
	expirationTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecExpirationTimeFieldOffset)
	hits := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecHitsFieldOffset)
	lastAccessTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastAccessTimeFieldOffset)
	lastStoredTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastStoredTimeFieldOffset)
	lastUpdateTime := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastUpdateTimeFieldOffset)
	version := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecVersionFieldOffset)
	ttl := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecTtlFieldOffset)
	maxIdle := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SimpleEntryViewCodecMaxIdleFieldOffset)

	key := DataCodec.Decode(frameIterator)
	value := DataCodec.Decode(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return core.NewSimpleEntryView(key, value, cost, creationTime, expirationTime, hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle)
}
