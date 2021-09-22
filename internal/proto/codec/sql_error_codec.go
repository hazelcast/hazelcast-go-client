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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	SqlErrorCodecCodeFieldOffset                     = 0
	SqlErrorCodecOriginatingMemberIdFieldOffset      = SqlErrorCodecCodeFieldOffset + proto.IntSizeInBytes
	SqlErrorCodecOriginatingMemberIdInitialFrameSize = SqlErrorCodecOriginatingMemberIdFieldOffset + proto.UUIDSizeInBytes
)

func EncodeSqlError(clientMessage *proto.ClientMessage, sqlError isql.Error) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, SqlErrorCodecOriginatingMemberIdInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SqlErrorCodecCodeFieldOffset, int32(sqlError.Code))
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, SqlErrorCodecOriginatingMemberIdFieldOffset, sqlError.OriginatingMemberId)
	clientMessage.AddFrame(initialFrame)

	CodecUtil.EncodeNullableForString(clientMessage, sqlError.Message)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeSqlError(frameIterator *proto.ForwardFrameIterator) isql.Error {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	code := FixSizedTypesCodec.DecodeInt(initialFrame.Content, SqlErrorCodecCodeFieldOffset)
	originatingMemberId := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, SqlErrorCodecOriginatingMemberIdFieldOffset)

	message := CodecUtil.DecodeNullableForString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return isql.Error{
		Code:                code,
		Message:             message,
		OriginatingMemberId: originatingMemberId,
	}
}
