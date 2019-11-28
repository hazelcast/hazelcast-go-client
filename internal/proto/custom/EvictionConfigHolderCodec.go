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

type EvictionConfigHolder struct {
size int
maxSizePolicy string
evictionPolicy string
comparatorClassName string
comparator serialization.Data
}

//@Generated("8165fc1333b41d44b30e19c60ed94331")
const (
    EvictionConfigHolderSizeFieldOffset = 0
    EvictionConfigHolderInitialFrameSize = EvictionConfigHolderSizeFieldOffset + bufutil.IntSizeInBytes
)

func EvictionConfigHolderEncode(clientMessage bufutil.ClientMessagex, evictionConfigHolder EvictionConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, EvictionConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, EvictionConfigHolderSizeFieldOffset, evictionConfigHolder.size)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, evictionConfigHolder.maxSizePolicy)
        StringCodec.encode(clientMessage, evictionConfigHolder.evictionPolicy)
        CodecUtil.encodeNullable(clientMessage, evictionConfigHolder.comparatorClassName, String.encode)
        CodecUtil.encodeNullable(clientMessage, evictionConfigHolder.comparator, Data.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func EvictionConfigHolderDecode(iterator bufutil.ClientMessagex)  *EvictionConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        size := bufutil.DecodeInt(initialFrame.Content, EvictionConfigHolderSizeFieldOffset)
        maxSizePolicy := StringCodec.decode(iterator)
        evictionPolicy := StringCodec.decode(iterator)
        comparatorClassName := CodecUtil.decodeNullable(iterator, String.decode)
        comparator := CodecUtil.decodeNullable(iterator, Data.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &EvictionConfigHolder { size, maxSizePolicy, evictionPolicy, comparatorClassName, comparator }
    }