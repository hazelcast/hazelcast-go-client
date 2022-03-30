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
	itype "github.com/hazelcast/hazelcast-go-client/internal/sql/types"
)

const (
	SqlCloseCodecRequestMessageType  = int32(0x210300)
	SqlCloseCodecResponseMessageType = int32(0x210301)

	SqlCloseCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Closes server-side query cursor.

func EncodeSqlCloseRequest(queryId itype.QueryID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, SqlCloseCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(SqlCloseCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeSqlQueryId(clientMessage, queryId)

	return clientMessage
}
