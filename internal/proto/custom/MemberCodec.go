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

type Member struct {
address Address
uuid UUID
attributes [string][string]Map
liteMember bool
}

//@Generated("5ec5c149c9757499b33efe85e8af4d40")
const (
    MemberUuidFieldOffset = 0
    MemberLiteMemberFieldOffset = MemberUuidFieldOffset + bufutil.UUIDSizeInBytes
    MemberInitialFrameSize = MemberLiteMemberFieldOffset + bufutil.BooleanSizeInBytes
)

func MemberEncode(clientMessage bufutil.ClientMessagex, member Member) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, MemberInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeUUID(initialFrame.Content, MemberUuidFieldOffset, member.uuid)
        bufutil.EncodeBoolean(initialFrame.Content, MemberLiteMemberFieldOffset, member.isliteMember)
        clientMessage.Add(initialFrame)
        AddressCodec.encode(clientMessage, member.address)
        MapCodec.encode(clientMessage, member.getAttributes(), String.encode, String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func MemberDecode(iterator bufutil.ClientMessagex)  *MemberImpl  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        uuid := bufutil.DecodeUUID(initialFrame.Content, MemberUuidFieldOffset)
        liteMember := bufutil.DecodeBoolean(initialFrame.Content, MemberLiteMemberFieldOffset)
        address := AddressCodec.decode(iterator)
        attributes := MapCodec.decode(iterator, String.decode, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &MemberImpl { address, uuid, attributes, liteMember }
    }