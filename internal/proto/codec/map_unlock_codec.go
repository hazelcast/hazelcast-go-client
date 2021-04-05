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
	// hex: 0x011300
	MapUnlockCodecRequestMessageType = int32(70400)
	// hex: 0x011301
	MapUnlockCodecResponseMessageType = int32(70401)

	MapUnlockCodecRequestThreadIdOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapUnlockCodecRequestReferenceIdOffset = MapUnlockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapUnlockCodecRequestInitialFrameSize  = MapUnlockCodecRequestReferenceIdOffset + proto.LongSizeInBytes
)

// Releases the lock for the specified key. It never blocks and returns immediately.
// If the current thread is the holder of this lock, then the hold count is decremented.If the hold count is zero,
// then the lock is released.  If the current thread is not the holder of this lock,
// then ILLEGAL_MONITOR_STATE is thrown.

func EncodeMapUnlockRequest(name string, key serialization.Data, threadId int64, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapUnlockCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapUnlockCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapUnlockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapUnlockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
