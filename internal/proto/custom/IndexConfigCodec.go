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

type IndexConfig struct {
name string
type int
attributes []String]
}

//@Generated("b54fa625f3abcc778808b7321ee83b49")
const (
    IndexConfigTypeFieldOffset = 0
    IndexConfigInitialFrameSize = IndexConfigTypeFieldOffset + bufutil.EnumSizeInBytes
)

func IndexConfigEncode(clientMessage bufutil.ClientMessagex, indexConfig IndexConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, IndexConfigInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeEnum(initialFrame.Content, IndexConfigTypeFieldOffset, indexConfig.type)
        clientMessage.Add(initialFrame)
        CodecUtil.encodeNullable(clientMessage, indexConfig.name, String.encode)
        ListMultiFrameCodec.encode(clientMessage, indexConfig.getAttributes(), String.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func IndexConfigDecode(iterator bufutil.ClientMessagex)  *IndexConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        type := bufutil.DecodeEnum(initialFrame.Content, IndexConfigTypeFieldOffset)
        name := CodecUtil.decodeNullable(iterator, String.decode)
        attributes := ListMultiFrameCodec.decode(iterator, String.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return CustomTypeFactory.createIndexConfig(name, type, attributes)
    }