/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	MemberInfoCodecUuidFieldOffset            = 0
	MemberInfoCodecLiteMemberFieldOffset      = MemberInfoCodecUuidFieldOffset + proto.UUIDSizeInBytes
	MemberInfoCodecLiteMemberInitialFrameSize = MemberInfoCodecLiteMemberFieldOffset + proto.BooleanSizeInBytes
)

type memberinfoCodec struct{}

var MemberInfoCodec memberinfoCodec

func (memberinfoCodec) Encode(clientMessage *proto.ClientMessage, memberInfo proto.MemberInfo) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, MemberInfoCodecLiteMemberInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MemberInfoCodecUuidFieldOffset, memberInfo.GetUuid())
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MemberInfoCodecLiteMemberFieldOffset, memberInfo.GetLiteMember())
	clientMessage.AddFrame(initialFrame)

	AddressCodec.Encode(clientMessage, memberInfo.GetAddress())
	MapCodec.EncodeForStringAndString(clientMessage, memberInfo.GetAttributes())
	MemberVersionCodec.Encode(clientMessage, memberInfo.GetVersion())
	MapCodec.EncodeForEndpointQualifierAndAddress(clientMessage, memberInfo.GetAddressMap())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (memberinfoCodec) Decode(frameIterator *proto.ForwardFrameIterator) proto.MemberInfo {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MemberInfoCodecUuidFieldOffset)
	liteMember := FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MemberInfoCodecLiteMemberFieldOffset)

	address := AddressCodec.Decode(frameIterator)
	attributes := MapCodec.DecodeForStringAndString(frameIterator)
	version := MemberVersionCodec.Decode(frameIterator)
	isAddressMapExists := false
	var addressMap interface{}
	if !frameIterator.PeekNext().IsEndFrame() {
		addressMap = MapCodec.DecodeForEndpointQualifierAndAddress(frameIterator)
		isAddressMapExists = true
	}
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return proto.NewMemberInfo(address, uuid, attributes, liteMember, version, isAddressMapExists, addressMap)
}
