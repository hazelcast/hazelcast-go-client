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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	// hex: 0x021400
	MultiMapForceUnlockCodecRequestMessageType = int32(136192)
	// hex: 0x021401
	MultiMapForceUnlockCodecResponseMessageType = int32(136193)

	MultiMapForceUnlockCodecRequestReferenceIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapForceUnlockCodecRequestInitialFrameSize  = MultiMapForceUnlockCodecRequestReferenceIdOffset + proto.LongSizeInBytes
)

// Releases the lock for the specified key regardless of the lock owner. It always successfully unlocks the key,
// never blocks and returns immediately.

func EncodeMultiMapForceUnlockRequest(name string, key *serialization.Data, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MultiMapForceUnlockCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapForceUnlockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapForceUnlockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
