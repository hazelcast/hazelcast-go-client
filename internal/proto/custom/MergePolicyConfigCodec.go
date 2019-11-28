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

type MergePolicyConfig struct {
policy string
batchSize int
}

//@Generated("1e75d4a6c47b117f58dffd3a1781b357")
const (
    MergePolicyConfigBatchSizeFieldOffset = 0
    MergePolicyConfigInitialFrameSize = MergePolicyConfigBatchSizeFieldOffset + bufutil.IntSizeInBytes
)

func MergePolicyConfigEncode(clientMessage bufutil.ClientMessagex, mergePolicyConfig MergePolicyConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, MergePolicyConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, MergePolicyConfigBatchSizeFieldOffset, mergePolicyConfig.batchSize)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, mergePolicyConfig.policy)

        clientMessage.Add(bufutil.EndFrame)
    }
func MergePolicyConfigDecode(iterator bufutil.ClientMessagex)  *MergePolicyConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        batchSize := bufutil.DecodeInt(initialFrame.Content, MergePolicyConfigBatchSizeFieldOffset)
        policy := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &MergePolicyConfig { policy, batchSize }
    }