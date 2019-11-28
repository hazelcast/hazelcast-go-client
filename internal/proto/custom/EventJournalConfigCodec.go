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

type EventJournalConfig struct {
enabled bool
capacity int
timeToLiveSeconds int
}

//@Generated("a1cd47b128a8f92fbfab1fe800f34865")
const (
    EventJournalConfigEnabledFieldOffset = 0
    EventJournalConfigCapacityFieldOffset = EventJournalConfigEnabledFieldOffset + bufutil.BooleanSizeInBytes
    EventJournalConfigTimeToLiveSecondsFieldOffset = EventJournalConfigCapacityFieldOffset + bufutil.IntSizeInBytes
    EventJournalConfigInitialFrameSize = EventJournalConfigTimeToLiveSecondsFieldOffset + bufutil.IntSizeInBytes
)

func EventJournalConfigEncode(clientMessage bufutil.ClientMessagex, eventJournalConfig EventJournalConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, EventJournalConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, EventJournalConfigEnabledFieldOffset, eventJournalConfig.isenabled)
        bufutil.EncodeInt(initialFrame.Content, EventJournalConfigCapacityFieldOffset, eventJournalConfig.capacity)
        bufutil.EncodeInt(initialFrame.Content, EventJournalConfigTimeToLiveSecondsFieldOffset, eventJournalConfig.timeToLiveSeconds)
        clientMessage.Add(initialFrame)

        clientMessage.Add(bufutil.EndFrame)
    }
func EventJournalConfigDecode(iterator bufutil.ClientMessagex)  *EventJournalConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        enabled := bufutil.DecodeBoolean(initialFrame.Content, EventJournalConfigEnabledFieldOffset)
        capacity := bufutil.DecodeInt(initialFrame.Content, EventJournalConfigCapacityFieldOffset)
        timeToLiveSeconds := bufutil.DecodeInt(initialFrame.Content, EventJournalConfigTimeToLiveSecondsFieldOffset)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createEventJournalConfig(enabled, capacity, timeToLiveSeconds)
    }