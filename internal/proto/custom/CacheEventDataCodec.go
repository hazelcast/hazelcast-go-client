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

type CacheEventData struct {
name string
cacheEventType int
dataKey serialization.Data
dataValue serialization.Data
dataOldValue serialization.Data
oldValueAvailable bool
}

//@Generated("6b5cec2287e2e74ea37b73897c9d7c02")
const (
    CacheEventDataCacheEventTypeFieldOffset = 0
    CacheEventDataOldValueAvailableFieldOffset = CacheEventDataCacheEventTypeFieldOffset + bufutil.EnumSizeInBytes
    CacheEventDataInitialFrameSize = CacheEventDataOldValueAvailableFieldOffset + bufutil.BooleanSizeInBytes
)

func CacheEventDataEncode(clientMessage bufutil.ClientMessagex, cacheEventData CacheEventData) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, CacheEventDataInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeEnum(initialFrame.Content, CacheEventDataCacheEventTypeFieldOffset, cacheEventData.cacheEventType)
        bufutil.EncodeBoolean(initialFrame.Content, CacheEventDataOldValueAvailableFieldOffset, cacheEventData.isoldValueAvailable)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, cacheEventData.name)
        CodecUtil.encodeNullable(clientMessage, cacheEventData.dataKey, Data.encode)
        CodecUtil.encodeNullable(clientMessage, cacheEventData.dataValue, Data.encode)
        CodecUtil.encodeNullable(clientMessage, cacheEventData.dataOldValue, Data.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func CacheEventDataDecode(iterator bufutil.ClientMessagex)  *CacheEventDataImpl  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        cacheEventType := bufutil.DecodeEnum(initialFrame.Content, CacheEventDataCacheEventTypeFieldOffset)
        oldValueAvailable := bufutil.DecodeBoolean(initialFrame.Content, CacheEventDataOldValueAvailableFieldOffset)
        name := StringCodec.decode(iterator)
        dataKey := CodecUtil.decodeNullable(iterator, Data.decode)
        dataValue := CodecUtil.decodeNullable(iterator, Data.decode)
        dataOldValue := CodecUtil.decodeNullable(iterator, Data.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createCacheEventData(name, cacheEventType, dataKey, dataValue, dataOldValue, oldValueAvailable)
    }