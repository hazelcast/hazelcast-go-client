/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package proto

import (
"bytes"
"github.com/hazelcast/hazelcast-go-client/serialization"
_ "github.com/hazelcast/hazelcast-go-client"
)

type SimpleEntryView struct {
key serialization.Data
value serialization.Data
cost int64
creationTime int64
expirationTime int64
hits int64
lastAccessTime int64
lastStoredTime int64
lastUpdateTime int64
version int64
ttl int64
maxIdle int64
}

//@Generated("6be4befee24cfcafbd1927f860ddbadf")
const (
    SimpleEntryViewCostFieldOffset = 0
    SimpleEntryViewCreationTimeFieldOffset = SimpleEntryViewCostFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewExpirationTimeFieldOffset = SimpleEntryViewCreationTimeFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewHitsFieldOffset = SimpleEntryViewExpirationTimeFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewLastAccessTimeFieldOffset = SimpleEntryViewHitsFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewLastStoredTimeFieldOffset = SimpleEntryViewLastAccessTimeFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewLastUpdateTimeFieldOffset = SimpleEntryViewLastStoredTimeFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewVersionFieldOffset = SimpleEntryViewLastUpdateTimeFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewTtlFieldOffset = SimpleEntryViewVersionFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewMaxIdleFieldOffset = SimpleEntryViewTtlFieldOffset + bufutil.LongSizeInBytes
    SimpleEntryViewInitialFrameSize = SimpleEntryViewMaxIdleFieldOffset + bufutil.LongSizeInBytes
)

func SimpleEntryViewEncode(clientMessage bufutil.ClientMessagex, simpleEntryView [serialization.Data][serialization.Data]SimpleEntryView) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, SimpleEntryViewInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewCostFieldOffset, simpleEntryView.cost)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewCreationTimeFieldOffset, simpleEntryView.creationTime)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewExpirationTimeFieldOffset, simpleEntryView.expirationTime)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewHitsFieldOffset, simpleEntryView.hits)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewLastAccessTimeFieldOffset, simpleEntryView.lastAccessTime)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewLastStoredTimeFieldOffset, simpleEntryView.lastStoredTime)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewLastUpdateTimeFieldOffset, simpleEntryView.lastUpdateTime)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewVersionFieldOffset, simpleEntryView.version)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewTtlFieldOffset, simpleEntryView.ttl)
        bufutil.EncodeLong(initialFrame.Content, SimpleEntryViewMaxIdleFieldOffset, simpleEntryView.maxIdle)
        clientMessage.Add(initialFrame)
        DataCodec.encode(clientMessage, simpleEntryView.key)
        DataCodec.encode(clientMessage, simpleEntryView.value)

        clientMessage.Add(bufutil.EndFrame)
    }
func SimpleEntryViewDecode(iterator bufutil.ClientMessagex)  *[serialization.Data][serialization.Data]SimpleEntryView  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        cost := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewCostFieldOffset)
        creationTime := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewCreationTimeFieldOffset)
        expirationTime := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewExpirationTimeFieldOffset)
        hits := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewHitsFieldOffset)
        lastAccessTime := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewLastAccessTimeFieldOffset)
        lastStoredTime := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewLastStoredTimeFieldOffset)
        lastUpdateTime := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewLastUpdateTimeFieldOffset)
        version := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewVersionFieldOffset)
        ttl := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewTtlFieldOffset)
        maxIdle := bufutil.DecodeLong(initialFrame.Content, SimpleEntryViewMaxIdleFieldOffset)
        key := DataCodec.decode(iterator)
        value := DataCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createSimpleEntryView(key, value, cost, creationTime, expirationTime, hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle)
    }