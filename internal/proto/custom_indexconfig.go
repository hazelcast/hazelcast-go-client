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
    "github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

type IndexConfig struct {
name string
_type int32
attributes []string
}

//@Generated("5338378742f5ec7a88ec3de27dc0c86d")
const (
    IndexConfigTypeFieldOffset = 0
    IndexConfigInitialFrameSize = IndexConfigTypeFieldOffset + bufutil.IntSizeInBytes
)

func IndexConfigCodecEncode(clientMessage *bufutil.ClientMessage, indexConfig IndexConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := &bufutil.Frame{Content: make([]byte, IndexConfigInitialFrameSize), Flags: bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, IndexConfigTypeFieldOffset, indexConfig._type)
        clientMessage.Add(initialFrame)
        bufutil.EncodeNullable(clientMessage, indexConfig.name, bufutil.StringCodecEncode)
        elements :=  indexConfig.attributes
         clientMessage.Add(bufutil.BeginFrame)
        for i := 0; i < len(elements) ; i++ {
            StringCodecEncode(clientMessage, elements[i])
        }

        clientMessage.Add(bufutil.EndFrame)
    }

func IndexConfigCodecDecode(iterator *bufutil.ForwardFrameIterator)  IndexConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        _type := bufutil.DecodeInt(initialFrame.Content, IndexConfigTypeFieldOffset)
        name := bufutil.DecodeNullable(iterator, bufutil.StringCodecDecode).(string)
        var attributes []string
         //begin frame, list
        iterator.Next()
        for !bufutil.NextFrameIsDataStructureEndFrame(iterator) {
            attributes = append(attributes, StringCodecDecode(iterator))
        }
        //end frame, list
        iterator.Next()
        bufutil.FastForwardToEndFrame(iterator)
        return IndexConfig { name, _type, attributes }
    }