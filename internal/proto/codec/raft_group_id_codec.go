/*
* Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	types2 "github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	RaftGroupIdCodecSeedFieldOffset    = 0
	RaftGroupIdCodecIdFieldOffset      = RaftGroupIdCodecSeedFieldOffset + proto.LongSizeInBytes
	RaftGroupIdCodecIdInitialFrameSize = RaftGroupIdCodecIdFieldOffset + proto.LongSizeInBytes
)

func EncodeRaftGroupId(clientMessage *proto.ClientMessage, raftGroupId types2.RaftGroupId) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, RaftGroupIdCodecIdInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset, int64(raftGroupId.Seed))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset, int64(raftGroupId.Id))
	clientMessage.AddFrame(initialFrame)

	EncodeString(clientMessage, raftGroupId.Name)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeRaftGroupId(frameIterator *proto.ForwardFrameIterator) types2.RaftGroupId {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	seed := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset)
	id := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset)

	name := DecodeString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return types2.RaftGroupId{
		CPGroupId: &types.CPGroupId{
			Name: name,
			Id:   id,
		},
		Seed: seed}
}
