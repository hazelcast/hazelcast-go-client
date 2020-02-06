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
)

type DistributedObjectInfo struct {
serviceName string
name string
}

//CONSTRUCTOR
func NewDistributedObjectInfo(serviceName string,name string) *DistributedObjectInfo {
return &DistributedObjectInfo{serviceName,name}
}


//GETTERS
func (x *DistributedObjectInfo) ServiceName() string {
    return x.serviceName
    }
func (x *DistributedObjectInfo) Name() string {
    return x.name
    }


//@Generated("17c4981c15c4fe6bb3476e079a179061")
const (
)

func DistributedObjectInfoCodecEncode(clientMessage *ClientMessage, distributedObjectInfo DistributedObjectInfo) {
        clientMessage.Add(BeginFrame)
        StringCodecEncode(clientMessage, distributedObjectInfo.serviceName)
        StringCodecEncode(clientMessage, distributedObjectInfo.name)

        clientMessage.Add(EndFrame)
    }

func DistributedObjectInfoCodecDecode(iterator *ForwardFrameIterator)  DistributedObjectInfo  {
        // begin frame
        iterator.Next()
        serviceName := StringCodecDecode(iterator)
        name := StringCodecDecode(iterator)
        FastForwardToEndFrame(iterator)
        return DistributedObjectInfo { serviceName, name }
    }