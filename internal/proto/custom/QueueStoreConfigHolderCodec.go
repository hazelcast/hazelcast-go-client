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

type QueueStoreConfigHolder struct {
className string
factoryClassName string
implementation serialization.Data
factoryImplementation serialization.Data
properties [string][string]Map
enabled bool
}

//@Generated("7492d58661517d317166e3f9f2cfd257")
const (
    QueueStoreConfigHolderEnabledFieldOffset = 0
    QueueStoreConfigHolderInitialFrameSize = QueueStoreConfigHolderEnabledFieldOffset + bufutil.BooleanSizeInBytes
)

func QueueStoreConfigHolderEncode(clientMessage bufutil.ClientMessagex, queueStoreConfigHolder QueueStoreConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, QueueStoreConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, QueueStoreConfigHolderEnabledFieldOffset, queueStoreConfigHolder.isenabled)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, queueStoreConfigHolder.className, String.encode)
        CodecUtil.encodeNullable(clientMessage, queueStoreConfigHolder.factoryClassName, String.encode)
        CodecUtil.encodeNullable(clientMessage, queueStoreConfigHolder.implementation, Data.encode)
        CodecUtil.encodeNullable(clientMessage, queueStoreConfigHolder.factoryImplementation, Data.encode)
        MapCodec.encodeNullable(clientMessage, queueStoreConfigHolder.getProperties(), String.encode, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func QueueStoreConfigHolderDecode(iterator bufutil.ClientMessagex)  *QueueStoreConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, QueueStoreConfigHolderEnabledFieldOffset)
        className := CodecUtil.decodeNullable(iterator, String.decode)
        factoryClassName := CodecUtil.decodeNullable(iterator, String.decode)
        implementation := CodecUtil.decodeNullable(iterator, Data.decode)
        factoryImplementation := CodecUtil.decodeNullable(iterator, Data.decode)
        properties := MapCodec.decodeNullable(iterator, String.decode, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &QueueStoreConfigHolder { className, factoryClassName, implementation, factoryImplementation, properties, enabled }
    }