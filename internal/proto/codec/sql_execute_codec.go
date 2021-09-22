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
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

const (
	SqlExecuteCodecRequestMessageType  = int32(0x210400)
	SqlExecuteCodecResponseMessageType = int32(0x210401)

	SqlExecuteCodecRequestTimeoutMillisOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	SqlExecuteCodecRequestCursorBufferSizeOffset   = SqlExecuteCodecRequestTimeoutMillisOffset + proto.LongSizeInBytes
	SqlExecuteCodecRequestExpectedResultTypeOffset = SqlExecuteCodecRequestCursorBufferSizeOffset + proto.IntSizeInBytes
	SqlExecuteCodecRequestInitialFrameSize         = SqlExecuteCodecRequestExpectedResultTypeOffset + proto.ByteSizeInBytes

	SqlExecuteResponseUpdateCountOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Starts execution of an SQL query (as of 4.2).

func EncodeSqlExecuteRequest(sql string, parameters []*iserialization.Data, timeoutMillis int64, cursorBufferSize int32, schema string, expectedResultType byte, queryId isql.QueryID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SqlExecuteCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, SqlExecuteCodecRequestTimeoutMillisOffset, timeoutMillis)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SqlExecuteCodecRequestCursorBufferSizeOffset, cursorBufferSize)
	FixSizedTypesCodec.EncodeByte(initialFrame.Content, SqlExecuteCodecRequestExpectedResultTypeOffset, expectedResultType)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SqlExecuteCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, sql)
	EncodeListMultiFrameContainsNullableForData(clientMessage, parameters)
	CodecUtil.EncodeNullableForString(clientMessage, schema)
	EncodeSqlQueryId(clientMessage, queryId)

	return clientMessage
}

func DecodeSqlExecuteResponse(clientMessage *proto.ClientMessage) (rowMetadata []sql.ColumnMetadata, rowPage isql.Page, updateCount int64, error isql.Error) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	updateCount = FixSizedTypesCodec.DecodeLong(initialFrame.Content, SqlExecuteResponseUpdateCountOffset)
	rowMetadata = DecodeNullableListMultiFrameForSqlColumnMetadata(frameIterator)
	rowPage = CodecUtil.DecodeNullableForSqlPage(frameIterator)
	error = CodecUtil.DecodeNullableForSqlError(frameIterator)

	return rowMetadata, rowPage, updateCount, error
}
