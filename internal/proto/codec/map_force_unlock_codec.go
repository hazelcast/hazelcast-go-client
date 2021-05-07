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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x013300
	MapForceUnlockCodecRequestMessageType = int32(78592)
	// hex: 0x013301
	MapForceUnlockCodecResponseMessageType = int32(78593)

	MapForceUnlockCodecRequestReferenceIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapForceUnlockCodecRequestInitialFrameSize  = MapForceUnlockCodecRequestReferenceIdOffset + proto.LongSizeInBytes
)

// Releases the lock for the specified key regardless of the lock owner.It always successfully unlocks the key,
// never blocks,and returns immediately.

func EncodeMapForceUnlockRequest(name string, key serialization.Data, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapForceUnlockCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapForceUnlockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapForceUnlockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
