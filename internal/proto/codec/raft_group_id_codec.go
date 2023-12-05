/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	RaftGroupIdCodecSeedFieldOffset    = 0
	RaftGroupIdCodecIdFieldOffset      = RaftGroupIdCodecSeedFieldOffset + proto.LongSizeInBytes
	RaftGroupIdCodecIdInitialFrameSize = RaftGroupIdCodecIdFieldOffset + proto.LongSizeInBytes
)

func EncodeRaftGroupId(clientMessage *proto.ClientMessage, raftGroupId types.RaftGroupID) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, RaftGroupIdCodecIdInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset, int64(raftGroupId.Seed))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset, int64(raftGroupId.Id))
	clientMessage.AddFrame(initialFrame)

	EncodeString(clientMessage, raftGroupId.Name)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeRaftGroupId(frameIterator *proto.ForwardFrameIterator) types.RaftGroupID {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	seed := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset)
	id := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset)

	name := DecodeString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return types.RaftGroupID{
		Name: name,
		Id:   id,
		Seed: seed,
	}
}
