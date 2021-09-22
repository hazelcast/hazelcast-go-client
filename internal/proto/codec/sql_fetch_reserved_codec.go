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
)

const (
	SqlFetch_reservedCodecRequestMessageType  = int32(0x210200)
	SqlFetch_reservedCodecResponseMessageType = int32(0x210201)

	SqlFetch_reservedCodecRequestCursorBufferSizeOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	SqlFetch_reservedCodecRequestInitialFrameSize       = SqlFetch_reservedCodecRequestCursorBufferSizeOffset + proto.IntSizeInBytes

	SqlFetch_reservedResponseRowPageLastOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Fetches the next row page.

func EncodeSqlFetch_reservedRequest(queryId isql.QueryID, cursorBufferSize int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SqlFetch_reservedCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SqlFetch_reservedCodecRequestCursorBufferSizeOffset, cursorBufferSize)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SqlFetch_reservedCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeSqlQueryId(clientMessage, queryId)

	return clientMessage
}

func DecodeSqlFetch_reservedResponse(clientMessage *proto.ClientMessage) (rowPage []*iserialization.Data, rowPageLast bool, error isql.Error) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	rowPageLast = FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SqlFetch_reservedResponseRowPageLastOffset)
	rowPage = DecodeNullableListMultiFrameForListCNData(frameIterator)
	error = CodecUtil.DecodeNullableForSqlError(frameIterator)

	return rowPage, rowPageLast, error
}
