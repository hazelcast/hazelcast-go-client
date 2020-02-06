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

type IndexConfig struct {
name string
_type int32
attributes []string
}

func NewIndexConfig(name string, _type int32, attributes []string) *IndexConfig {
    return &IndexConfig{name: name, _type: _type, attributes: attributes}
}

//@Generated("5338378742f5ec7a88ec3de27dc0c86d")
const (
    IndexConfigTypeFieldOffset = 0
    IndexConfigInitialFrameSize = IndexConfigTypeFieldOffset + IntSizeInBytes
)

func IndexConfigCodecEncode(clientMessage *ClientMessage, indexConfig *IndexConfig) {
        clientMessage.Add(BeginFrame)
        initialFrame := &Frame{Content: make([]byte, IndexConfigInitialFrameSize), Flags: UnfragmentedMessage}
        EncodeInt(initialFrame.Content, IndexConfigTypeFieldOffset, indexConfig._type)
        clientMessage.Add(initialFrame)
        EncodeNullable(clientMessage, indexConfig.name, StringCodecEncode)
        elements :=  indexConfig.attributes
         clientMessage.Add(BeginFrame)
        for i := 0; i < len(elements) ; i++ {
            StringCodecEncode(clientMessage, elements[i])
        }

        clientMessage.Add(EndFrame)
    }

func IndexConfigCodecDecode(iterator *ForwardFrameIterator)  IndexConfig  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        _type := DecodeInt(initialFrame.Content, IndexConfigTypeFieldOffset)
        name := DecodeNullable(iterator, StringCodecDecode).(string)
        var attributes []string
         //begin frame, list
        iterator.Next()
        for !NextFrameIsDataStructureEndFrame(iterator) {
            attributes = append(attributes, StringCodecDecode(iterator))
        }
        //end frame, list
        iterator.Next()
        FastForwardToEndFrame(iterator)
        return IndexConfig { name, _type, attributes }
    }