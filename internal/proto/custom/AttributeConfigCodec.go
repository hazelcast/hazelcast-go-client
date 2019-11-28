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

type AttributeConfig struct {
name string
extractorClassName string
}

//@Generated("1d5a8981510f432302056f65229dd301")
const (
)

func AttributeConfigEncode(clientMessage bufutil.ClientMessagex, attributeConfig AttributeConfig) {
        clientMessage.Add(bufutil.BeginFrame)
        StringCodec.encode(clientMessage, attributeConfig.name)
        StringCodec.encode(clientMessage, attributeConfig.extractorClassName)

        clientMessage.Add(bufutil.EndFrame)
    }
func AttributeConfigDecode(iterator bufutil.ClientMessagex)  *AttributeConfig  {
        // begin frame
        iterator.Next()
        name := StringCodec.decode(iterator)
        extractorClassName := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &AttributeConfig { name, extractorClassName }
    }