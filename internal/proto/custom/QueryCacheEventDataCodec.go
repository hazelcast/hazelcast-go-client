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

type QueryCacheEventData struct {
dataKey serialization.Data
dataNewValue serialization.Data
sequence int64
eventType int
partitionId int
}

//@Generated("550b063f0793a9e685fbdd33733925ae")
const (
    QueryCacheEventDataSequenceFieldOffset = 0
    QueryCacheEventDataEventTypeFieldOffset = QueryCacheEventDataSequenceFieldOffset + bufutil.LongSizeInBytes
    QueryCacheEventDataPartitionIdFieldOffset = QueryCacheEventDataEventTypeFieldOffset + bufutil.IntSizeInBytes
    QueryCacheEventDataInitialFrameSize = QueryCacheEventDataPartitionIdFieldOffset + bufutil.IntSizeInBytes
)

func QueryCacheEventDataEncode(clientMessage bufutil.ClientMessagex, queryCacheEventData QueryCacheEventData) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, QueryCacheEventDataInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeLong(initialFrame.Content, QueryCacheEventDataSequenceFieldOffset, queryCacheEventData.sequence)
        bufutil.EncodeInt(initialFrame.Content, QueryCacheEventDataEventTypeFieldOffset, queryCacheEventData.eventType)
        bufutil.EncodeInt(initialFrame.Content, QueryCacheEventDataPartitionIdFieldOffset, queryCacheEventData.partitionId)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, queryCacheEventData.dataKey, Data.encode)
        CodecUtil.encodeNullable(clientMessage, queryCacheEventData.dataNewValue, Data.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func QueryCacheEventDataDecode(iterator bufutil.ClientMessagex)  *DefaultQueryCacheEventData  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        sequence := bufutil.DecodeLong(initialFrame.Content, QueryCacheEventDataSequenceFieldOffset)
        eventType := bufutil.DecodeInt(initialFrame.Content, QueryCacheEventDataEventTypeFieldOffset)
        partitionId := bufutil.DecodeInt(initialFrame.Content, QueryCacheEventDataPartitionIdFieldOffset)
        dataKey := CodecUtil.decodeNullable(iterator, Data.decode)
        dataNewValue := CodecUtil.decodeNullable(iterator, Data.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createQueryCacheEventData(dataKey, dataNewValue, sequence, eventType, partitionId)
    }