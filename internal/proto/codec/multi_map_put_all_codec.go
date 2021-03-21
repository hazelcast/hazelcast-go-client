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
	// hex: 0x021700
	MultiMapPutAllCodecRequestMessageType = int32(136960)
	// hex: 0x021701
	MultiMapPutAllCodecResponseMessageType = int32(136961)

	MultiMapPutAllCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Copies all of the mappings from the specified map to this MultiMap. The effect of this call is
// equivalent to that of calling put(k, v) on this MultiMap iteratively for each value in the mapping from key k to value
// v in the specified MultiMap. The behavior of this operation is undefined if the specified map is modified while the
// operation is in progress.

func EncodeMultiMapPutAllRequest(name string, entries []proto.Pair) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MultiMapPutAllCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapPutAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListForDataAndListData(clientMessage, entries)

	return clientMessage
}
