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

type HotRestartConfig struct {
enabled bool
fsync bool
}

//@Generated("4d37c777fd2b76ac050246f5eceebfc2")
const (
    HotRestartConfigEnabledFieldOffset = 0
    HotRestartConfigFsyncFieldOffset = HotRestartConfigEnabledFieldOffset + bufutil.BooleanSizeInBytes
    HotRestartConfigInitialFrameSize = HotRestartConfigFsyncFieldOffset + bufutil.BooleanSizeInBytes
)

func HotRestartConfigEncode(clientMessage bufutil.ClientMessagex, hotRestartConfig HotRestartConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, HotRestartConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, HotRestartConfigEnabledFieldOffset, hotRestartConfig.isenabled)
        bufutil.EncodeBoolean(initialFrame.Content, HotRestartConfigFsyncFieldOffset, hotRestartConfig.isfsync)
        clientMessage.Add(initialFrame)

        clientMessage.Add(bufutil.EndFrame)
    }
func HotRestartConfigDecode(iterator bufutil.ClientMessagex)  *HotRestartConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, HotRestartConfigEnabledFieldOffset)
        fsync := bufutil.DecodeBoolean(initialFrame.Content, HotRestartConfigFsyncFieldOffset)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createHotRestartConfig(enabled, fsync)
    }