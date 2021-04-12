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
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	MemberVersionCodecMajorFieldOffset      = 0
	MemberVersionCodecMinorFieldOffset      = MemberVersionCodecMajorFieldOffset + proto.ByteSizeInBytes
	MemberVersionCodecPatchFieldOffset      = MemberVersionCodecMinorFieldOffset + proto.ByteSizeInBytes
	MemberVersionCodecPatchInitialFrameSize = MemberVersionCodecPatchFieldOffset + proto.ByteSizeInBytes
)

/*
type memberversionCodec struct {}

var MemberVersionCodec memberversionCodec
*/

func EncodeMemberVersion(clientMessage *proto.ClientMessage, memberVersion cluster.MemberVersion) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, MemberVersionCodecPatchInitialFrameSize))
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, MemberVersionCodecMajorFieldOffset, memberVersion.Major())
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, MemberVersionCodecMinorFieldOffset, memberVersion.Minor())
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, MemberVersionCodecPatchFieldOffset, memberVersion.Patch())
	clientMessage.AddFrame(initialFrame)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeMemberVersion(frameIterator *proto.ForwardFrameIterator) cluster.MemberVersion {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	major := FixSizedTypesCodec.DecodeByte(initialFrame.Content, MemberVersionCodecMajorFieldOffset)
	minor := FixSizedTypesCodec.DecodeByte(initialFrame.Content, MemberVersionCodecMinorFieldOffset)
	patch := FixSizedTypesCodec.DecodeByte(initialFrame.Content, MemberVersionCodecPatchFieldOffset)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return cluster.NewMemberVersion(major, minor, patch)
}
