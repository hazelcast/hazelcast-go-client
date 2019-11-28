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

type ScheduledTaskHandler struct {
address Address
partitionId int
schedulerName string
taskName string
}

//@Generated("ecbe3510efa5965869891bc89db3a027")
const (
    ScheduledTaskHandlerPartitionIdFieldOffset = 0
    ScheduledTaskHandlerInitialFrameSize = ScheduledTaskHandlerPartitionIdFieldOffset + bufutil.IntSizeInBytes
)

func ScheduledTaskHandlerEncode(clientMessage bufutil.ClientMessagex, scheduledTaskHandler ScheduledTaskHandler) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, ScheduledTaskHandlerInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, ScheduledTaskHandlerPartitionIdFieldOffset, scheduledTaskHandler.partitionId)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, scheduledTaskHandler.address, Address.encode)
        StringCodec.encode(clientMessage, scheduledTaskHandler.schedulerName)
        StringCodec.encode(clientMessage, scheduledTaskHandler.taskName)

        clientMessage.Add(bufutil.EndFrame)
    }
func ScheduledTaskHandlerDecode(iterator bufutil.ClientMessagex)  *ScheduledTaskHandlerImpl  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        partitionId := bufutil.DecodeInt(initialFrame.Content, ScheduledTaskHandlerPartitionIdFieldOffset)
        address := CodecUtil.decodeNullable(iterator, Address.decode)
        schedulerName := StringCodec.decode(iterator)
        taskName := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &ScheduledTaskHandlerImpl { address, partitionId, schedulerName, taskName }
    }