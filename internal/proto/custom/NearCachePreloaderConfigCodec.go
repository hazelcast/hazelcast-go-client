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

type NearCachePreloaderConfig struct {
enabled bool
directory string
storeInitialDelaySeconds int
storeIntervalSeconds int
}

//@Generated("360921b60ca1853fd2566f67412c3d9e")
const (
    NearCachePreloaderConfigEnabledFieldOffset = 0
    NearCachePreloaderConfigStoreInitialDelaySecondsFieldOffset = NearCachePreloaderConfigEnabledFieldOffset + bufutil.BooleanSizeInBytes
    NearCachePreloaderConfigStoreIntervalSecondsFieldOffset = NearCachePreloaderConfigStoreInitialDelaySecondsFieldOffset + bufutil.IntSizeInBytes
    NearCachePreloaderConfigInitialFrameSize = NearCachePreloaderConfigStoreIntervalSecondsFieldOffset + bufutil.IntSizeInBytes
)

func NearCachePreloaderConfigEncode(clientMessage bufutil.ClientMessagex, nearCachePreloaderConfig NearCachePreloaderConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, NearCachePreloaderConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, NearCachePreloaderConfigEnabledFieldOffset, nearCachePreloaderConfig.isenabled)
        bufutil.EncodeInt(initialFrame.Content, NearCachePreloaderConfigStoreInitialDelaySecondsFieldOffset, nearCachePreloaderConfig.storeInitialDelaySeconds)
        bufutil.EncodeInt(initialFrame.Content, NearCachePreloaderConfigStoreIntervalSecondsFieldOffset, nearCachePreloaderConfig.storeIntervalSeconds)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, nearCachePreloaderConfig.directory)

        clientMessage.Add(bufutil.EndFrame)
    }
func NearCachePreloaderConfigDecode(iterator bufutil.ClientMessagex)  *NearCachePreloaderConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, NearCachePreloaderConfigEnabledFieldOffset)
        storeInitialDelaySeconds := bufutil.DecodeInt(initialFrame.Content, NearCachePreloaderConfigStoreInitialDelaySecondsFieldOffset)
        storeIntervalSeconds := bufutil.DecodeInt(initialFrame.Content, NearCachePreloaderConfigStoreIntervalSecondsFieldOffset)
        directory := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createNearCachePreloaderConfig(enabled, directory, storeInitialDelaySeconds, storeIntervalSeconds)
    }