// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

const (
	// hex: 0x010C00
	MapTryPutCodecRequestMessageType = int32(68608)
	// hex: 0x010C01
	MapTryPutCodecResponseMessageType = int32(68609)

	MapTryPutCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapTryPutCodecRequestTimeoutOffset    = MapTryPutCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapTryPutCodecRequestInitialFrameSize = MapTryPutCodecRequestTimeoutOffset + proto.LongSizeInBytes

	MapTryPutResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Tries to put the given key and value into this map within a specified timeout value. If this method returns false,
// it means that the caller thread could not acquire the lock for the key within the timeout duration,
// thus the put operation is not successful.
type mapTryPutCodec struct{}

var MapTryPutCodec mapTryPutCodec

func (mapTryPutCodec) EncodeRequest(name string, key serialization.Data, value serialization.Data, threadId int64, timeout int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapTryPutCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryPutCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryPutCodecRequestTimeoutOffset, timeout)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapTryPutCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, key)
	DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (mapTryPutCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapTryPutResponseResponseOffset)
}
