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

type CacheSimpleEntryListenerConfig struct {
oldValueRequired bool
synchronous bool
cacheEntryListenerFactory string
cacheEntryEventFilterFactory string
}

//@Generated("5cc61ff18c561b8490f22cabf6b5bbf2")
const (
    CacheSimpleEntryListenerConfigOldValueRequiredFieldOffset = 0
    CacheSimpleEntryListenerConfigSynchronousFieldOffset = CacheSimpleEntryListenerConfigOldValueRequiredFieldOffset + bufutil.BooleanSizeInBytes
    CacheSimpleEntryListenerConfigInitialFrameSize = CacheSimpleEntryListenerConfigSynchronousFieldOffset + bufutil.BooleanSizeInBytes
)

func CacheSimpleEntryListenerConfigEncode(clientMessage bufutil.ClientMessagex, cacheSimpleEntryListenerConfig CacheSimpleEntryListenerConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, CacheSimpleEntryListenerConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, CacheSimpleEntryListenerConfigOldValueRequiredFieldOffset, cacheSimpleEntryListenerConfig.isoldValueRequired)
        bufutil.EncodeBoolean(initialFrame.Content, CacheSimpleEntryListenerConfigSynchronousFieldOffset, cacheSimpleEntryListenerConfig.issynchronous)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, cacheSimpleEntryListenerConfig.cacheEntryListenerFactory, String.encode)
        CodecUtil.encodeNullable(clientMessage, cacheSimpleEntryListenerConfig.cacheEntryEventFilterFactory, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func CacheSimpleEntryListenerConfigDecode(iterator bufutil.ClientMessagex)  *CacheSimpleEntryListenerConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        oldValueRequired := bufutil.DecodeBoolean(initialFrame.Content, CacheSimpleEntryListenerConfigOldValueRequiredFieldOffset)
        synchronous := bufutil.DecodeBoolean(initialFrame.Content, CacheSimpleEntryListenerConfigSynchronousFieldOffset)
        cacheEntryListenerFactory := CodecUtil.decodeNullable(iterator, String.decode)
        cacheEntryEventFilterFactory := CodecUtil.decodeNullable(iterator, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createCacheSimpleEntryListenerConfig(oldValueRequired, synchronous, cacheEntryListenerFactory, cacheEntryEventFilterFactory)
    }