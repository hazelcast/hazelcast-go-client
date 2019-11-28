/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package proto

import (
"bytes"
"github.com/hazelcast/hazelcast-go-client/serialization"
_ "github.com/hazelcast/hazelcast-go-client"
)

type RaftGroupId struct {
name string
seed int64
id int64
}

//@Generated("56db6eeffb2d950b334c951cae88bc4d")
const (
    RaftGroupIdSeedFieldOffset = 0
    RaftGroupIdIdFieldOffset = RaftGroupIdSeedFieldOffset + bufutil.LongSizeInBytes
    RaftGroupIdInitialFrameSize = RaftGroupIdIdFieldOffset + bufutil.LongSizeInBytes
)

func RaftGroupIdEncode(clientMessage bufutil.ClientMessagex, raftGroupId RaftGroupId) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, RaftGroupIdInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeLong(initialFrame.Content, RaftGroupIdSeedFieldOffset, raftGroupId.seed)
        bufutil.EncodeLong(initialFrame.Content, RaftGroupIdIdFieldOffset, raftGroupId.id)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, raftGroupId.name)

        clientMessage.Add(bufutil.EndFrame)
    }
func RaftGroupIdDecode(iterator bufutil.ClientMessagex)  *RaftGroupId  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        seed := bufutil.DecodeLong(initialFrame.Content, RaftGroupIdSeedFieldOffset)
        id := bufutil.DecodeLong(initialFrame.Content, RaftGroupIdIdFieldOffset)
        name := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &RaftGroupId { name, seed, id }
    }