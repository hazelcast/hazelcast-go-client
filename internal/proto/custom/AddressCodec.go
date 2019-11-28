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

type Address struct {
host string
port int
}

//@Generated("7fc11307a3a2f58b1e55111861fb2513")
const (
    AddressPortFieldOffset = 0
    AddressInitialFrameSize = AddressPortFieldOffset + bufutil.IntSizeInBytes
)

func AddressEncode(clientMessage bufutil.ClientMessagex, address Address) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, AddressInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, AddressPortFieldOffset, address.port)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, address.host)

        clientMessage.Add(bufutil.EndFrame)
    }
func AddressDecode(iterator bufutil.ClientMessagex)  *Address  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        port := bufutil.DecodeInt(initialFrame.Content, AddressPortFieldOffset)
        host := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createAddress(host, port)
    }