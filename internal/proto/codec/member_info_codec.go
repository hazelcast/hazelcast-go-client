/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	MemberInfoCodecUuidFieldOffset            = 0
	MemberInfoCodecLiteMemberFieldOffset      = MemberInfoCodecUuidFieldOffset + proto.UUIDSizeInBytes
	MemberInfoCodecLiteMemberInitialFrameSize = MemberInfoCodecLiteMemberFieldOffset + proto.BooleanSizeInBytes
)

/*
type memberinfoCodec struct {}

var MemberInfoCodec memberinfoCodec
*/

func EncodeMemberInfo(clientMessage *proto.ClientMessage, memberInfo cluster.MemberInfo) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, MemberInfoCodecLiteMemberInitialFrameSize))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MemberInfoCodecUuidFieldOffset, memberInfo.UUID)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MemberInfoCodecLiteMemberFieldOffset, memberInfo.LiteMember)
	clientMessage.AddFrame(initialFrame)

	EncodeAddress(clientMessage, memberInfo.Address)
	EncodeMapForStringAndString(clientMessage, memberInfo.Attributes)
	EncodeMemberVersion(clientMessage, memberInfo.Version)
	EncodeMapForEndpointQualifierAndAddress(clientMessage, memberInfo.AddressMap)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeMemberInfo(frameIterator *proto.ForwardFrameIterator) cluster.MemberInfo {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MemberInfoCodecUuidFieldOffset)
	liteMember := FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MemberInfoCodecLiteMemberFieldOffset)

	address := DecodeAddress(frameIterator)
	attributes := DecodeMapForStringAndString(frameIterator)
	version := DecodeMemberVersion(frameIterator)
	isAddressMapExists := false
	var addressMap interface{}
	if !frameIterator.PeekNext().IsEndFrame() {
		addressMap = DecodeMapForEndpointQualifierAndAddress(frameIterator)
		isAddressMapExists = true
	}
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return NewMemberInfo(address, uuid, attributes, liteMember, version, isAddressMapExists, addressMap)
}
