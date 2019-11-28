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

type NearCacheConfigHolder struct {
name string
inMemoryFormat string
serializeKeys bool
invalidateOnChange bool
timeToLiveSeconds int
maxIdleSeconds int
evictionConfigHolder EvictionConfigHolder
cacheLocalEntries bool
localUpdatePolicy string
preloaderConfig NearCachePreloaderConfig
}

//@Generated("8756b2b6edeb672db7efd14940a2f49a")
const (
    NearCacheConfigHolderSerializeKeysFieldOffset = 0
    NearCacheConfigHolderInvalidateOnChangeFieldOffset = NearCacheConfigHolderSerializeKeysFieldOffset + bufutil.BooleanSizeInBytes
    NearCacheConfigHolderTimeToLiveSecondsFieldOffset = NearCacheConfigHolderInvalidateOnChangeFieldOffset + bufutil.BooleanSizeInBytes
    NearCacheConfigHolderMaxIdleSecondsFieldOffset = NearCacheConfigHolderTimeToLiveSecondsFieldOffset + bufutil.IntSizeInBytes
    NearCacheConfigHolderCacheLocalEntriesFieldOffset = NearCacheConfigHolderMaxIdleSecondsFieldOffset + bufutil.IntSizeInBytes
    NearCacheConfigHolderInitialFrameSize = NearCacheConfigHolderCacheLocalEntriesFieldOffset + bufutil.BooleanSizeInBytes
)

func NearCacheConfigHolderEncode(clientMessage bufutil.ClientMessagex, nearCacheConfigHolder NearCacheConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, NearCacheConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, NearCacheConfigHolderSerializeKeysFieldOffset, nearCacheConfigHolder.isserializeKeys)
        bufutil.EncodeBoolean(initialFrame.Content, NearCacheConfigHolderInvalidateOnChangeFieldOffset, nearCacheConfigHolder.isinvalidateOnChange)
        bufutil.EncodeInt(initialFrame.Content, NearCacheConfigHolderTimeToLiveSecondsFieldOffset, nearCacheConfigHolder.timeToLiveSeconds)
        bufutil.EncodeInt(initialFrame.Content, NearCacheConfigHolderMaxIdleSecondsFieldOffset, nearCacheConfigHolder.maxIdleSeconds)
        bufutil.EncodeBoolean(initialFrame.Content, NearCacheConfigHolderCacheLocalEntriesFieldOffset, nearCacheConfigHolder.iscacheLocalEntries)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, nearCacheConfigHolder.name)
        StringCodec.encode(clientMessage, nearCacheConfigHolder.inMemoryFormat)
        EvictionConfigHolderCodec.encode(clientMessage, nearCacheConfigHolder.evictionConfigHolder)
        StringCodec.encode(clientMessage, nearCacheConfigHolder.localUpdatePolicy)
        CodecUtil.encodeNullable(clientMessage, nearCacheConfigHolder.preloaderConfig, NearCachePreloaderConfig.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func NearCacheConfigHolderDecode(iterator bufutil.ClientMessagex)  *NearCacheConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        serializeKeys := bufutil.DecodeBoolean(initialFrame.Content, NearCacheConfigHolderSerializeKeysFieldOffset)
        invalidateOnChange := bufutil.DecodeBoolean(initialFrame.Content, NearCacheConfigHolderInvalidateOnChangeFieldOffset)
        timeToLiveSeconds := bufutil.DecodeInt(initialFrame.Content, NearCacheConfigHolderTimeToLiveSecondsFieldOffset)
        maxIdleSeconds := bufutil.DecodeInt(initialFrame.Content, NearCacheConfigHolderMaxIdleSecondsFieldOffset)
        cacheLocalEntries := bufutil.DecodeBoolean(initialFrame.Content, NearCacheConfigHolderCacheLocalEntriesFieldOffset)
        name := StringCodec.decode(iterator)
        inMemoryFormat := StringCodec.decode(iterator)
        evictionConfigHolder := EvictionConfigHolderCodec.decode(iterator)
        localUpdatePolicy := StringCodec.decode(iterator)
        preloaderConfig := CodecUtil.decodeNullable(iterator, NearCachePreloaderConfig.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &NearCacheConfigHolder { name, inMemoryFormat, serializeKeys, invalidateOnChange, timeToLiveSeconds, maxIdleSeconds, evictionConfigHolder, cacheLocalEntries, localUpdatePolicy, preloaderConfig }
    }