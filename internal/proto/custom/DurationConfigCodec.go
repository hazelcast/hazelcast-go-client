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

type DurationConfig struct {
durationAmount int64
timeUnit int
}

//@Generated("a2aed8eae1d9419a01b32cd0f8b35768")
const (
    DurationConfigDurationAmountFieldOffset = 0
    DurationConfigTimeUnitFieldOffset = DurationConfigDurationAmountFieldOffset + bufutil.LongSizeInBytes
    DurationConfigInitialFrameSize = DurationConfigTimeUnitFieldOffset + bufutil.EnumSizeInBytes
)

func DurationConfigEncode(clientMessage bufutil.ClientMessagex, durationConfig DurationConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, DurationConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeLong(initialFrame.Content, DurationConfigDurationAmountFieldOffset, durationConfig.durationAmount)
        bufutil.EncodeEnum(initialFrame.Content, DurationConfigTimeUnitFieldOffset, durationConfig.timeUnit)
        clientMessage.Add(initialFrame)

        clientMessage.Add(bufutil.EndFrame)
    }
func DurationConfigDecode(iterator bufutil.ClientMessagex)  *DurationConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        durationAmount := bufutil.DecodeLong(initialFrame.Content, DurationConfigDurationAmountFieldOffset)
        timeUnit := bufutil.DecodeEnum(initialFrame.Content, DurationConfigTimeUnitFieldOffset)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createDurationConfig(durationAmount, timeUnit)
    }