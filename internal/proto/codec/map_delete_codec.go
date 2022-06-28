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
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	// hex: 0x010900
	MapDeleteCodecRequestMessageType = int32(67840)
	// hex: 0x010901
	MapDeleteCodecResponseMessageType = int32(67841)

	MapDeleteCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapDeleteCodecRequestInitialFrameSize = MapDeleteCodecRequestThreadIdOffset + proto.LongSizeInBytes
)

// Removes the mapping for a key from this map if it is present.Unlike remove(Object), this operation does not return
// the removed value, which avoids the serialization cost of the returned value.If the removed value will not be used,
// a delete operation is preferred over a remove operation for better performance. The map will not contain a mapping
// for the specified key once the call returns.
// This method breaks the contract of EntryListener. When an entry is removed by delete(), it fires an EntryEvent
// with a null oldValue. Also, a listeners with predicates will have null values, so only keys can be queried via predicates

func EncodeMapDeleteRequest(name string, key iserialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, MapDeleteCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapDeleteCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapDeleteCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
