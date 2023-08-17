/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
)

const (
	ClientFetchSchemaCodecRequestMessageType  = int32(0x001400)
	ClientFetchSchemaCodecResponseMessageType = int32(0x001401)

	ClientFetchSchemaCodecRequestSchemaIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	ClientFetchSchemaCodecRequestInitialFrameSize = ClientFetchSchemaCodecRequestSchemaIdOffset + proto.LongSizeInBytes
)

// Fetches a schema from the cluster with the given schemaId

func EncodeClientFetchSchemaRequest(schemaId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, ClientFetchSchemaCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeLong(initialFrame.Content, ClientFetchSchemaCodecRequestSchemaIdOffset, schemaId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ClientFetchSchemaCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	return clientMessage
}

func DecodeClientFetchSchemaResponse(clientMessage *proto.ClientMessage) *iserialization.Schema {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return DecodeNullableForSchema(frameIterator)
}
