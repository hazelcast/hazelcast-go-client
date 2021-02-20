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
	// hex: 0x021000
	MultiMapLockCodecRequestMessageType = int32(135168)
	// hex: 0x021001
	MultiMapLockCodecResponseMessageType = int32(135169)

	MultiMapLockCodecRequestThreadIdOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapLockCodecRequestTtlOffset         = MultiMapLockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MultiMapLockCodecRequestReferenceIdOffset = MultiMapLockCodecRequestTtlOffset + proto.LongSizeInBytes
	MultiMapLockCodecRequestInitialFrameSize  = MultiMapLockCodecRequestReferenceIdOffset + proto.LongSizeInBytes
)

// Acquires the lock for the specified key for the specified lease time. After the lease time, the lock will be
// released. If the lock is not available, then the current thread becomes disabled for thread scheduling
// purposes and lies dormant until the lock has been acquired. Scope of the lock is for this map only. The acquired
// lock is only for the key in this map.Locks are re-entrant, so if the key is locked N times, then it should be
// unlocked N times before another thread can acquire it.
type multimapLockCodec struct{}

var MultiMapLockCodec multimapLockCodec

func (multimapLockCodec) EncodeRequest(name string, key serialization.Data, threadId int64, ttl int64, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MultiMapLockCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapLockCodecRequestThreadIdOffset, threadId)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapLockCodecRequestTtlOffset, ttl)
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapLockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapLockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	StringCodec.Encode(clientMessage, name)
	DataCodec.Encode(clientMessage, key)

	return clientMessage
}
