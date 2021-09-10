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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
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

/*
type simpleentryviewCodec struct {}

var SimpleEntryViewCodec simpleentryviewCodec
*/

func EncodeSimpleEntryView(clientMessage *proto.ClientMessage, simpleEntryView *types.SimpleEntryView) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, SimpleEntryViewCodecMaxIdleInitialFrameSize))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecCostFieldOffset, int64(simpleEntryView.Cost))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecCreationTimeFieldOffset, int64(simpleEntryView.CreationTime))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecExpirationTimeFieldOffset, int64(simpleEntryView.ExpirationTime))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecHitsFieldOffset, int64(simpleEntryView.Hits))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastAccessTimeFieldOffset, int64(simpleEntryView.LastAccessTime))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastStoredTimeFieldOffset, int64(simpleEntryView.LastStoredTime))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecLastUpdateTimeFieldOffset, int64(simpleEntryView.LastUpdateTime))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecVersionFieldOffset, int64(simpleEntryView.Version))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecTtlFieldOffset, int64(simpleEntryView.TTL))
	EncodeLong(initialFrame.Content, SimpleEntryViewCodecMaxIdleFieldOffset, int64(simpleEntryView.MaxIdle))
	clientMessage.AddFrame(initialFrame)

	EncodeData(clientMessage, simpleEntryView.Key)
	EncodeData(clientMessage, simpleEntryView.Value)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeSimpleEntryView(frameIterator *proto.ForwardFrameIterator) *types.SimpleEntryView {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	cost := DecodeLong(initialFrame.Content, SimpleEntryViewCodecCostFieldOffset)
	creationTime := DecodeLong(initialFrame.Content, SimpleEntryViewCodecCreationTimeFieldOffset)
	expirationTime := DecodeLong(initialFrame.Content, SimpleEntryViewCodecExpirationTimeFieldOffset)
	hits := DecodeLong(initialFrame.Content, SimpleEntryViewCodecHitsFieldOffset)
	lastAccessTime := DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastAccessTimeFieldOffset)
	lastStoredTime := DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastStoredTimeFieldOffset)
	lastUpdateTime := DecodeLong(initialFrame.Content, SimpleEntryViewCodecLastUpdateTimeFieldOffset)
	version := DecodeLong(initialFrame.Content, SimpleEntryViewCodecVersionFieldOffset)
	ttl := DecodeLong(initialFrame.Content, SimpleEntryViewCodecTtlFieldOffset)
	maxIdle := DecodeLong(initialFrame.Content, SimpleEntryViewCodecMaxIdleFieldOffset)

	key := DecodeData(frameIterator)
	value := DecodeData(frameIterator)
	FastForwardToEndFrame(frameIterator)
	return types.NewSimpleEntryView(key, value, cost, creationTime, expirationTime, hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle)
}
