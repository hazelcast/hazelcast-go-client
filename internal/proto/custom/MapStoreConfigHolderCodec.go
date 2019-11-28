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

type MapStoreConfigHolder struct {
enabled bool
writeCoalescing bool
writeDelaySeconds int
writeBatchSize int
className string
implementation serialization.Data
factoryClassName string
factoryImplementation serialization.Data
properties [string][string]Map
initialLoadMode string
}

//@Generated("b30248d828def3bd6b969330bfe879d5")
const (
    MapStoreConfigHolderEnabledFieldOffset = 0
    MapStoreConfigHolderWriteCoalescingFieldOffset = MapStoreConfigHolderEnabledFieldOffset + bufutil.BooleanSizeInBytes
    MapStoreConfigHolderWriteDelaySecondsFieldOffset = MapStoreConfigHolderWriteCoalescingFieldOffset + bufutil.BooleanSizeInBytes
    MapStoreConfigHolderWriteBatchSizeFieldOffset = MapStoreConfigHolderWriteDelaySecondsFieldOffset + bufutil.IntSizeInBytes
    MapStoreConfigHolderInitialFrameSize = MapStoreConfigHolderWriteBatchSizeFieldOffset + bufutil.IntSizeInBytes
)

func MapStoreConfigHolderEncode(clientMessage bufutil.ClientMessagex, mapStoreConfigHolder MapStoreConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, MapStoreConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, MapStoreConfigHolderEnabledFieldOffset, mapStoreConfigHolder.isenabled)
        bufutil.EncodeBoolean(initialFrame.Content, MapStoreConfigHolderWriteCoalescingFieldOffset, mapStoreConfigHolder.iswriteCoalescing)
        bufutil.EncodeInt(initialFrame.Content, MapStoreConfigHolderWriteDelaySecondsFieldOffset, mapStoreConfigHolder.writeDelaySeconds)
        bufutil.EncodeInt(initialFrame.Content, MapStoreConfigHolderWriteBatchSizeFieldOffset, mapStoreConfigHolder.writeBatchSize)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.className, String.encode)
        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.implementation, Data.encode)
        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.factoryClassName, String.encode)
        CodecUtil.encodeNullable(clientMessage, mapStoreConfigHolder.factoryImplementation, Data.encode)
        MapCodec.encodeNullable(clientMessage, mapStoreConfigHolder.getProperties(), String.encode, String.encode)
        StringCodec.encode(clientMessage, mapStoreConfigHolder.initialLoadMode)

        clientMessage.Add(bufutil.EndFrame)
    }
func MapStoreConfigHolderDecode(iterator bufutil.ClientMessagex)  *MapStoreConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, MapStoreConfigHolderEnabledFieldOffset)
        writeCoalescing := bufutil.DecodeBoolean(initialFrame.Content, MapStoreConfigHolderWriteCoalescingFieldOffset)
        writeDelaySeconds := bufutil.DecodeInt(initialFrame.Content, MapStoreConfigHolderWriteDelaySecondsFieldOffset)
        writeBatchSize := bufutil.DecodeInt(initialFrame.Content, MapStoreConfigHolderWriteBatchSizeFieldOffset)
        className := CodecUtil.decodeNullable(iterator, String.decode)
        implementation := CodecUtil.decodeNullable(iterator, Data.decode)
        factoryClassName := CodecUtil.decodeNullable(iterator, String.decode)
        factoryImplementation := CodecUtil.decodeNullable(iterator, Data.decode)
        properties := MapCodec.decodeNullable(iterator, String.decode, String.decode)
        initialLoadMode := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &MapStoreConfigHolder { enabled, writeCoalescing, writeDelaySeconds, writeBatchSize, className, implementation, factoryClassName, factoryImplementation, properties, initialLoadMode }
    }