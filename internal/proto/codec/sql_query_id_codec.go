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
	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
)

const (
	SqlQueryIdCodecMemberIdHighFieldOffset    = 0
	SqlQueryIdCodecMemberIdLowFieldOffset     = SqlQueryIdCodecMemberIdHighFieldOffset + proto.LongSizeInBytes
	SqlQueryIdCodecLocalIdHighFieldOffset     = SqlQueryIdCodecMemberIdLowFieldOffset + proto.LongSizeInBytes
	SqlQueryIdCodecLocalIdLowFieldOffset      = SqlQueryIdCodecLocalIdHighFieldOffset + proto.LongSizeInBytes
	SqlQueryIdCodecLocalIdLowInitialFrameSize = SqlQueryIdCodecLocalIdLowFieldOffset + proto.LongSizeInBytes
)

func EncodeSqlQueryId(clientMessage *proto.ClientMessage, sqlQueryId isql.QueryID) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, SqlQueryIdCodecLocalIdLowInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SqlQueryIdCodecMemberIdHighFieldOffset, int64(sqlQueryId.MemberIdHigh))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SqlQueryIdCodecMemberIdLowFieldOffset, int64(sqlQueryId.MemberIdLow))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SqlQueryIdCodecLocalIdHighFieldOffset, int64(sqlQueryId.LocalIdHigh))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SqlQueryIdCodecLocalIdLowFieldOffset, int64(sqlQueryId.LocalIdLow))
	clientMessage.AddFrame(initialFrame)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeSqlQueryId(frameIterator *proto.ForwardFrameIterator) isql.QueryID {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	memberIdHigh := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SqlQueryIdCodecMemberIdHighFieldOffset)
	memberIdLow := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SqlQueryIdCodecMemberIdLowFieldOffset)
	localIdHigh := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SqlQueryIdCodecLocalIdHighFieldOffset)
	localIdLow := FixSizedTypesCodec.DecodeLong(initialFrame.Content, SqlQueryIdCodecLocalIdLowFieldOffset)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return isql.QueryID{
		MemberIdHigh: memberIdHigh,
		MemberIdLow:  memberIdLow,
		LocalIdHigh:  localIdHigh,
		LocalIdLow:   localIdLow,
	}
}
