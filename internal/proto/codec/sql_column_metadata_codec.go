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
	"github.com/hazelcast/hazelcast-go-client/sql"
)

const (
	SqlColumnMetadataCodecTypeFieldOffset          = 0
	SqlColumnMetadataCodecNullableFieldOffset      = SqlColumnMetadataCodecTypeFieldOffset + proto.IntSizeInBytes
	SqlColumnMetadataCodecNullableInitialFrameSize = SqlColumnMetadataCodecNullableFieldOffset + proto.BooleanSizeInBytes
)

func EncodeSqlColumnMetadata(clientMessage *proto.ClientMessage, sqlColumnMetadata sql.ColumnMetadata) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrameWith(make([]byte, SqlColumnMetadataCodecNullableInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, SqlColumnMetadataCodecTypeFieldOffset, int32(sqlColumnMetadata.Type))
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, SqlColumnMetadataCodecNullableFieldOffset, sqlColumnMetadata.Nullable)
	clientMessage.AddFrame(initialFrame)

	EncodeString(clientMessage, sqlColumnMetadata.Name)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeSqlColumnMetadata(frameIterator *proto.ForwardFrameIterator) sql.ColumnMetadata {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	_type := FixSizedTypesCodec.DecodeInt(initialFrame.Content, SqlColumnMetadataCodecTypeFieldOffset)
	nullable := false
	if len(initialFrame.Content) >= SqlColumnMetadataCodecNullableFieldOffset+proto.BooleanSizeInBytes {
		nullable = FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, SqlColumnMetadataCodecNullableFieldOffset)
	}

	name := DecodeString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return sql.ColumnMetadata{
		Name:     name,
		Type:     sql.ColumnType(_type),
		Nullable: nullable,
	}
}
