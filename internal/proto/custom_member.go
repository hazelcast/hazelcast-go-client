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
uuid string
attributes map[string]string
liteMember bool
}

//@Generated("34c97214585c5977eaec7c893156d7ff")
const (
    MemberUuidFieldOffset = 0
    MemberLiteMemberFieldOffset = MemberUuidFieldOffset + bufutil.UUIDSizeInBytes
    MemberInitialFrameSize = MemberLiteMemberFieldOffset + bufutil.BooleanSizeInBytes
)

func MemberCodecEncode(clientMessage *bufutil.ClientMessagex, member proto.Member) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := &bufutil.Frame{make([]byte, MemberInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeUUID(initialFrame.Content, MemberUuidFieldOffset, member.uuid)
        bufutil.EncodeBoolean(initialFrame.Content, MemberLiteMemberFieldOffset, member.liteMember)
        clientMessage.Add(initialFrame)
        bufutil.AddressCodecEncode(clientMessage, member.address)
        bufutil.MapCodecEncode(clientMessage, member.attributes, bufutil.StringCodecEncode, bufutil.StringCodecEncode)

        clientMessage.Add(bufutil.EndFrame)
    }

func MemberCodecDecode(iterator *bufutil.ForwardFrameIterator)  Member  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        uuid := bufutil.DecodeUUID(initialFrame.Content, MemberUuidFieldOffset)
        liteMember := bufutil.DecodeBoolean(initialFrame.Content, MemberLiteMemberFieldOffset)
        address := bufutil.AddressCodecDecode(iterator)
        attributes := bufutil.MapCodecDecode(iterator, bufutil.StringCodecDecode, bufutil.StringCodecDecode)

        bufutil.FastForwardToEndFrame(iterator)

        return Member { address, uuid, attributes, liteMember }
    }