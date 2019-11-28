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

type WanReplicationRef struct {
name string
mergePolicy string
filters []String]
republishingEnabled bool
}

//@Generated("beaa32a92bbe2f21659801a121a8e9e3")
const (
    WanReplicationRefRepublishingEnabledFieldOffset = 0
    WanReplicationRefInitialFrameSize = WanReplicationRefRepublishingEnabledFieldOffset + bufutil.BooleanSizeInBytes
)

func WanReplicationRefEncode(clientMessage bufutil.ClientMessagex, wanReplicationRef WanReplicationRef) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, WanReplicationRefInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeBoolean(initialFrame.Content, WanReplicationRefRepublishingEnabledFieldOffset, wanReplicationRef.isrepublishingEnabled)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, wanReplicationRef.name)
        StringCodec.encode(clientMessage, wanReplicationRef.mergePolicy)
        ListMultiFrameCodec.encodeNullable(clientMessage, wanReplicationRef.getFilters(), String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func WanReplicationRefDecode(iterator bufutil.ClientMessagex)  *WanReplicationRef  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        republishingEnabled := bufutil.DecodeBoolean(initialFrame.Content, WanReplicationRefRepublishingEnabledFieldOffset)
        name := StringCodec.decode(iterator)
        mergePolicy := StringCodec.decode(iterator)
        filters := ListMultiFrameCodec.decodeNullable(iterator, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &WanReplicationRef { name, mergePolicy, filters, republishingEnabled }
    }