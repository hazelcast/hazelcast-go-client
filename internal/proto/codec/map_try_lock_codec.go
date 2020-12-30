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
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x011100
	MapTryLockCodecRequestMessageType = int32(69888)
	// hex: 0x011101
	MapTryLockCodecResponseMessageType = int32(69889)

	MapTryLockCodecRequestThreadIdOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapTryLockCodecRequestLeaseOffset       = MapTryLockCodecRequestThreadIdOffset + proto.LongSizeInBytes
	MapTryLockCodecRequestTimeoutOffset     = MapTryLockCodecRequestLeaseOffset + proto.LongSizeInBytes
	MapTryLockCodecRequestReferenceIdOffset = MapTryLockCodecRequestTimeoutOffset + proto.LongSizeInBytes
	MapTryLockCodecRequestInitialFrameSize  = MapTryLockCodecRequestReferenceIdOffset + proto.LongSizeInBytes

	MapTryLockResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Tries to acquire the lock for the specified key for the specified lease time.After lease time, the lock will be
// released.If the lock is not available, then the current thread becomes disabled for thread scheduling
// purposes and lies dormant until one of two things happens the lock is acquired by the current thread, or
// the specified waiting time elapses.
type mapTryLockCodec struct{}

var MapTryLockCodec mapTryLockCodec

func (mapTryLockCodec) EncodeRequest(name string, key serialization.Data, threadId int64, lease int64, timeout int64, referenceId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapTryLockCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryLockCodecRequestThreadIdOffset, threadId)
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryLockCodecRequestLeaseOffset, lease)
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryLockCodecRequestTimeoutOffset, timeout)
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MapTryLockCodecRequestReferenceIdOffset, referenceId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapTryLockCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)

	return clientMessage
}

func (mapTryLockCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapTryLockResponseResponseOffset)
}
