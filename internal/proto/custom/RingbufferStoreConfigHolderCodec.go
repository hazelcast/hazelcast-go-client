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

type RingbufferStoreConfigHolder struct {
className string
factoryClassName string
implementation serialization.Data
factoryImplementation serialization.Data
properties [string][string]Map
enabled bool
}

//@Generated("1950893a9f7b9962299af7f04dcbe2d9")
const (
    RingbufferStoreConfigHolderEnabledFieldOffset = 0
    RingbufferStoreConfigHolderInitialFrameSize = RingbufferStoreConfigHolderEnabledFieldOffset + bufutil.BooleanSizeInBytes
)

func RingbufferStoreConfigHolderEncode(clientMessage bufutil.ClientMessagex, ringbufferStoreConfigHolder RingbufferStoreConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, RingbufferStoreConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, RingbufferStoreConfigHolderEnabledFieldOffset, ringbufferStoreConfigHolder.isenabled)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.className, String.encode)
        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.factoryClassName, String.encode)
        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.implementation, Data.encode)
        CodecUtil.encodeNullable(clientMessage, ringbufferStoreConfigHolder.factoryImplementation, Data.encode)
        MapCodec.encodeNullable(clientMessage, ringbufferStoreConfigHolder.getProperties(), String.encode, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func RingbufferStoreConfigHolderDecode(iterator bufutil.ClientMessagex)  *RingbufferStoreConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, RingbufferStoreConfigHolderEnabledFieldOffset)
        className := CodecUtil.decodeNullable(iterator, String.decode)
        factoryClassName := CodecUtil.decodeNullable(iterator, String.decode)
        implementation := CodecUtil.decodeNullable(iterator, Data.decode)
        factoryImplementation := CodecUtil.decodeNullable(iterator, Data.decode)
        properties := MapCodec.decodeNullable(iterator, String.decode, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &RingbufferStoreConfigHolder { className, factoryClassName, implementation, factoryImplementation, properties, enabled }
    }