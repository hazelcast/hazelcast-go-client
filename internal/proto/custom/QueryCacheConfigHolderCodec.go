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

type QueryCacheConfigHolder struct {
batchSize int
bufferSize int
delaySeconds int
includeValue bool
populate bool
coalesce bool
inMemoryFormat string
name string
predicateConfigHolder PredicateConfigHolder
evictionConfigHolder EvictionConfigHolder
listenerConfigs []]ListenerConfigHolder
indexConfigs []IndexConfig
}

//@Generated("6e3fdb2dc2385c20c78c26c5ccbf4ba8")
const (
    QueryCacheConfigHolderBatchSizeFieldOffset = 0
    QueryCacheConfigHolderBufferSizeFieldOffset = QueryCacheConfigHolderBatchSizeFieldOffset + bufutil.IntSizeInBytes
    QueryCacheConfigHolderDelaySecondsFieldOffset = QueryCacheConfigHolderBufferSizeFieldOffset + bufutil.IntSizeInBytes
    QueryCacheConfigHolderIncludeValueFieldOffset = QueryCacheConfigHolderDelaySecondsFieldOffset + bufutil.IntSizeInBytes
    QueryCacheConfigHolderPopulateFieldOffset = QueryCacheConfigHolderIncludeValueFieldOffset + bufutil.BooleanSizeInBytes
    QueryCacheConfigHolderCoalesceFieldOffset = QueryCacheConfigHolderPopulateFieldOffset + bufutil.BooleanSizeInBytes
    QueryCacheConfigHolderInitialFrameSize = QueryCacheConfigHolderCoalesceFieldOffset + bufutil.BooleanSizeInBytes
)

func QueryCacheConfigHolderEncode(clientMessage bufutil.ClientMessagex, queryCacheConfigHolder QueryCacheConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, QueryCacheConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, QueryCacheConfigHolderBatchSizeFieldOffset, queryCacheConfigHolder.batchSize)
        bufutil.EncodeInt(initialFrame.Content, QueryCacheConfigHolderBufferSizeFieldOffset, queryCacheConfigHolder.bufferSize)
        bufutil.EncodeInt(initialFrame.Content, QueryCacheConfigHolderDelaySecondsFieldOffset, queryCacheConfigHolder.delaySeconds)
        bufutil.EncodeBoolean(initialFrame.Content, QueryCacheConfigHolderIncludeValueFieldOffset, queryCacheConfigHolder.isincludeValue)
        bufutil.EncodeBoolean(initialFrame.Content, QueryCacheConfigHolderPopulateFieldOffset, queryCacheConfigHolder.ispopulate)
        bufutil.EncodeBoolean(initialFrame.Content, QueryCacheConfigHolderCoalesceFieldOffset, queryCacheConfigHolder.iscoalesce)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, queryCacheConfigHolder.inMemoryFormat)
        StringCodec.encode(clientMessage, queryCacheConfigHolder.name)
        PredicateConfigHolderCodec.encode(clientMessage, queryCacheConfigHolder.predicateConfigHolder)
        EvictionConfigHolderCodec.encode(clientMessage, queryCacheConfigHolder.evictionConfigHolder)
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigHolder.getListenerConfigs(), ListenerConfigHolder.encode)
        ListMultiFrameCodec.encodeNullable(clientMessage, queryCacheConfigHolder.getIndexConfigs(), IndexConfig.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func QueryCacheConfigHolderDecode(iterator bufutil.ClientMessagex)  *QueryCacheConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        batchSize := bufutil.DecodeInt(initialFrame.Content, QueryCacheConfigHolderBatchSizeFieldOffset)
        bufferSize := bufutil.DecodeInt(initialFrame.Content, QueryCacheConfigHolderBufferSizeFieldOffset)
        delaySeconds := bufutil.DecodeInt(initialFrame.Content, QueryCacheConfigHolderDelaySecondsFieldOffset)
        includeValue := bufutil.DecodeBoolean(initialFrame.Content, QueryCacheConfigHolderIncludeValueFieldOffset)
        populate := bufutil.DecodeBoolean(initialFrame.Content, QueryCacheConfigHolderPopulateFieldOffset)
        coalesce := bufutil.DecodeBoolean(initialFrame.Content, QueryCacheConfigHolderCoalesceFieldOffset)
        inMemoryFormat := StringCodec.decode(iterator)
        name := StringCodec.decode(iterator)
        predicateConfigHolder := PredicateConfigHolderCodec.decode(iterator)
        evictionConfigHolder := EvictionConfigHolderCodec.decode(iterator)
        listenerConfigs := ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolder.decode)
        indexConfigs := ListMultiFrameCodec.decodeNullable(iterator, IndexConfig.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &QueryCacheConfigHolder { batchSize, bufferSize, delaySeconds, includeValue, populate, coalesce, inMemoryFormat, name, predicateConfigHolder, evictionConfigHolder, listenerConfigs, indexConfigs }
    }