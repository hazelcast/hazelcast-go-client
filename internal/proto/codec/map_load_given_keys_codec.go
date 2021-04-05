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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x012100
	MapLoadGivenKeysCodecRequestMessageType = int32(73984)
	// hex: 0x012101
	MapLoadGivenKeysCodecResponseMessageType = int32(73985)

	MapLoadGivenKeysCodecRequestReplaceExistingValuesOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapLoadGivenKeysCodecRequestInitialFrameSize            = MapLoadGivenKeysCodecRequestReplaceExistingValuesOffset + proto.BooleanSizeInBytes
)

// Loads the given keys. This is a batch load operation so that an implementation can optimize the multiple loads.

func EncodeMapLoadGivenKeysRequest(name string, keys []serialization.Data, replaceExistingValues bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapLoadGivenKeysCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapLoadGivenKeysCodecRequestReplaceExistingValuesOffset, replaceExistingValues)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapLoadGivenKeysCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeListMultiFrameForData(clientMessage, keys)

	return clientMessage
}
