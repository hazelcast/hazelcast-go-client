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
	// hex: 0x011000
	MapLockCodecRequestMessageType = int32(69632)
	// hex: 0x011001
	MapLockCodecResponseMessageType = int32(69633)

	MapLockCodecRequestThreadIdOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapLockCodecRequestTtlOffset         = MapLockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapLockCodecRequestReferenceIdOffset = MapLockCodecRequestTtlOffset + proto.LongSizeInBytes
	MapLockCodecRequestInitialFrameSize  = MapLockCodecRequestReferenceIdOffset + proto.LongSizeInBytes
)

// Acquires the lock for the specified lease time.After lease time, lock will be released.If the lock is not
// available then the current thread becomes disabled for thread scheduling purposes and lies dormant until the lock
// has been acquired.
// Scope of the lock is this map only. Acquired lock is only for the key in this map. Locks are re-entrant,
// so if the key is locked N times then it should be unlocked N times before another thread can acquire it.

func EncodeMapLockRequest(name string, key serialization.Data, threadId int64, ttl int64, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrameWith(make([]byte, MapLockCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapLockCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapLockCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapLockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapLockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}
