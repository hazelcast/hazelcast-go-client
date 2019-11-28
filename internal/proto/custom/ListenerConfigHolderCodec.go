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

type ListenerConfigHolder struct {
listenerType int
listenerImplementation serialization.Data
className string
includeValue bool
local bool
}

//@Generated("daa628c7339ea9152ab6a4d5868c96ca")
const (
    ListenerConfigHolderListenerTypeFieldOffset = 0
    ListenerConfigHolderIncludeValueFieldOffset = ListenerConfigHolderListenerTypeFieldOffset + bufutil.IntSizeInBytes
    ListenerConfigHolderLocalFieldOffset = ListenerConfigHolderIncludeValueFieldOffset + bufutil.BooleanSizeInBytes
    ListenerConfigHolderInitialFrameSize = ListenerConfigHolderLocalFieldOffset + bufutil.BooleanSizeInBytes
)

func ListenerConfigHolderEncode(clientMessage bufutil.ClientMessagex, listenerConfigHolder ListenerConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, ListenerConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, ListenerConfigHolderListenerTypeFieldOffset, listenerConfigHolder.listenerType)
        bufutil.EncodeBoolean(initialFrame.Content, ListenerConfigHolderIncludeValueFieldOffset, listenerConfigHolder.isincludeValue)
        bufutil.EncodeBoolean(initialFrame.Content, ListenerConfigHolderLocalFieldOffset, listenerConfigHolder.islocal)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, listenerConfigHolder.listenerImplementation, Data.encode)
        CodecUtil.encodeNullable(clientMessage, listenerConfigHolder.className, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func ListenerConfigHolderDecode(iterator bufutil.ClientMessagex)  *ListenerConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        listenerType := bufutil.DecodeInt(initialFrame.Content, ListenerConfigHolderListenerTypeFieldOffset)
        includeValue := bufutil.DecodeBoolean(initialFrame.Content, ListenerConfigHolderIncludeValueFieldOffset)
        local := bufutil.DecodeBoolean(initialFrame.Content, ListenerConfigHolderLocalFieldOffset)
        listenerImplementation := CodecUtil.decodeNullable(iterator, Data.decode)
        className := CodecUtil.decodeNullable(iterator, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &ListenerConfigHolder { listenerType, listenerImplementation, className, includeValue, local }
    }