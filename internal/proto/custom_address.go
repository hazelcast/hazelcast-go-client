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
    "github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type Address struct {
host string
port int32
}

//@Generated("7aa314f34af9f77e9609527fe69101bf")
const (
    AddressPortFieldOffset = 0
    AddressInitialFrameSize = AddressPortFieldOffset + bufutil.IntSizeInBytes
)

func AddressCodecEncode(clientMessage *bufutil.ClientMessage, address Address) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := &bufutil.Frame{Content: make([]byte, AddressInitialFrameSize), Flags: bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, AddressPortFieldOffset, address.port)
        clientMessage.Add(initialFrame)
        bufutil.StringCodecEncode(clientMessage, address.host)

        clientMessage.Add(bufutil.EndFrame)
    }

func AddressCodecDecode(iterator *bufutil.ForwardFrameIterator)  Address  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        port := bufutil.DecodeInt(initialFrame.Content, AddressPortFieldOffset)
        host := bufutil.StringCodecDecode(iterator)
        bufutil.FastForwardToEndFrame(iterator)
        return Address { host, port }
    }