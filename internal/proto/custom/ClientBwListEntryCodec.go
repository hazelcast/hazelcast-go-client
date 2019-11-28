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

type ClientBwListEntry struct {
type int
value string
}

//@Generated("fac5b606cf81888b2f81a191944fb92d")
const (
    ClientBwListEntryTypeFieldOffset = 0
    ClientBwListEntryInitialFrameSize = ClientBwListEntryTypeFieldOffset + bufutil.EnumSizeInBytes
)

func ClientBwListEntryEncode(clientMessage bufutil.ClientMessagex, clientBwListEntry ClientBwListEntryDTO) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, ClientBwListEntryInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeEnum(initialFrame.Content, ClientBwListEntryTypeFieldOffset, clientBwListEntry.type)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, clientBwListEntry.value)

        clientMessage.Add(bufutil.EndFrame)
    }
func ClientBwListEntryDecode(iterator bufutil.ClientMessagex)  *ClientBwListEntryDTO  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        type := bufutil.DecodeEnum(initialFrame.Content, ClientBwListEntryTypeFieldOffset)
        value := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createClientBwListEntry(type, value)
    }