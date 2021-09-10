/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
)

const (
	// hex: 0x012C00
	MapPutAllCodecRequestMessageType = int32(76800)
	// hex: 0x012C01
	MapPutAllCodecResponseMessageType = int32(76801)

	MapPutAllCodecRequestTriggerMapLoaderOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapPutAllCodecRequestInitialFrameSize       = MapPutAllCodecRequestTriggerMapLoaderOffset + proto.BooleanSizeInBytes
)

// Copies all of the mappings from the specified map to this map (optional operation).The effect of this call is
// equivalent to that of calling put(Object,Object) put(k, v) on this map once for each mapping from key k to value
// v in the specified map.The behavior of this operation is undefined if the specified map is modified while the
// operation is in progress.
// Please note that all the keys in the request should belong to the partition id to which this request is being sent, all keys
// matching to a different partition id shall be ignored. The API implementation using this request may need to send multiple
// of these request messages for filling a request for a key set if the keys belong to different partitions.

func EncodeMapPutAllRequest(name string, entries []proto.Pair, triggerMapLoader bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapPutAllCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	EncodeBoolean(initialFrame.Content, MapPutAllCodecRequestTriggerMapLoaderOffset, triggerMapLoader)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapPutAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListForDataAndData(clientMessage, entries)

	return clientMessage
}
