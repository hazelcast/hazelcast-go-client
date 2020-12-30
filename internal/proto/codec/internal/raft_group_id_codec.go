/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	RaftGroupIdCodecSeedFieldOffset    = 0
	RaftGroupIdCodecIdFieldOffset      = RaftGroupIdCodecSeedFieldOffset + proto.LongSizeInBytes
	RaftGroupIdCodecIdInitialFrameSize = RaftGroupIdCodecIdFieldOffset + proto.LongSizeInBytes
)

type raftgroupidCodec struct{}

var RaftGroupIdCodec raftgroupidCodec

func (raftgroupidCodec) Encode(clientMessage *proto.ClientMessage, raftGroupId proto.RaftGroupId) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, RaftGroupIdCodecIdInitialFrameSize))
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset, raftGroupId.GetSeed())
	FixSizedTypesCodec.EncodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset, raftGroupId.GetId())
	clientMessage.AddFrame(initialFrame)

	StringCodec.Encode(clientMessage, raftGroupId.GetName())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (raftgroupidCodec) Decode(frameIterator *proto.ForwardFrameIterator) proto.RaftGroupId {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	seed := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecSeedFieldOffset)
	id := FixSizedTypesCodec.DecodeLong(initialFrame.Content, RaftGroupIdCodecIdFieldOffset)

	name := StringCodec.Decode(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return proto.NewRaftGroupId(name, seed, id)
}
