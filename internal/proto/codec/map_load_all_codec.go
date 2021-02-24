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
)

const (
	// hex: 0x012000
	MapLoadAllCodecRequestMessageType = int32(73728)
	// hex: 0x012001
	MapLoadAllCodecResponseMessageType = int32(73729)

	MapLoadAllCodecRequestReplaceExistingValuesOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapLoadAllCodecRequestInitialFrameSize            = MapLoadAllCodecRequestReplaceExistingValuesOffset + proto.BooleanSizeInBytes
)

// Loads all keys into the store. This is a batch load operation so that an implementation can optimize the multiple loads.

func EncodeMapLoadAllRequest(name string, replaceExistingValues bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapLoadAllCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapLoadAllCodecRequestReplaceExistingValuesOffset, replaceExistingValues)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapLoadAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)

	return clientMessage
}
