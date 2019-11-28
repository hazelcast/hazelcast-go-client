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

type MerkleTreeConfig struct {
enabled bool
depth int
}

//@Generated("3ecc55e87dc9ee1b08e3df8be3a10c4b")
const (
    MerkleTreeConfigEnabledFieldOffset = 0
    MerkleTreeConfigDepthFieldOffset = MerkleTreeConfigEnabledFieldOffset + bufutil.BooleanSizeInBytes
    MerkleTreeConfigInitialFrameSize = MerkleTreeConfigDepthFieldOffset + bufutil.IntSizeInBytes
)

func MerkleTreeConfigEncode(clientMessage bufutil.ClientMessagex, merkleTreeConfig MerkleTreeConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, MerkleTreeConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, MerkleTreeConfigEnabledFieldOffset, merkleTreeConfig.isenabled)
        bufutil.EncodeInt(initialFrame.Content, MerkleTreeConfigDepthFieldOffset, merkleTreeConfig.depth)
        clientMessage.Add(initialFrame)

        clientMessage.Add(bufutil.EndFrame)
    }
func MerkleTreeConfigDecode(iterator bufutil.ClientMessagex)  *MerkleTreeConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, MerkleTreeConfigEnabledFieldOffset)
        depth := bufutil.DecodeInt(initialFrame.Content, MerkleTreeConfigDepthFieldOffset)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createMerkleTreeConfig(enabled, depth)
    }