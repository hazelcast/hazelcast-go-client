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

type TimedExpiryPolicyFactoryConfig struct {
expiryPolicyType int
durationConfig DurationConfig
}

//@Generated("aff4f4e46cc838466b651dfc54568749")
const (
    TimedExpiryPolicyFactoryConfigExpiryPolicyTypeFieldOffset = 0
    TimedExpiryPolicyFactoryConfigInitialFrameSize = TimedExpiryPolicyFactoryConfigExpiryPolicyTypeFieldOffset + bufutil.EnumSizeInBytes
)

func TimedExpiryPolicyFactoryConfigEncode(clientMessage bufutil.ClientMessagex, timedExpiryPolicyFactoryConfig TimedExpiryPolicyFactoryConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, TimedExpiryPolicyFactoryConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeEnum(initialFrame.Content, TimedExpiryPolicyFactoryConfigExpiryPolicyTypeFieldOffset, timedExpiryPolicyFactoryConfig.expiryPolicyType)
        clientMessage.Add(initialFrame)
        DurationConfigCodec.encode(clientMessage, timedExpiryPolicyFactoryConfig.durationConfig)

        clientMessage.Add(bufutil.EndFrame)
    }
func TimedExpiryPolicyFactoryConfigDecode(iterator bufutil.ClientMessagex)  *TimedExpiryPolicyFactoryConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        expiryPolicyType := bufutil.DecodeEnum(initialFrame.Content, TimedExpiryPolicyFactoryConfigExpiryPolicyTypeFieldOffset)
        durationConfig := DurationConfigCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createTimedExpiryPolicyFactoryConfig(expiryPolicyType, durationConfig)
    }