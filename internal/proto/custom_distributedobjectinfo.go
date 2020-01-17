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

type DistributedObjectInfo struct {
serviceName string
name string
}

//@Generated("9be8b60cf2c9c5b479e729ea876de7e9")
const (
)

func DistributedObjectInfoCodecEncode(clientMessage *bufutil.ClientMessagex, distributedObjectInfo DistributedObjectInfo) {
        clientMessage.Add(bufutil.BeginFrame)
        bufutil.StringCodecEncode(clientMessage, distributedObjectInfo.serviceName)
        bufutil.StringCodecEncode(clientMessage, distributedObjectInfo.name)

        clientMessage.Add(bufutil.EndFrame)
    }

func DistributedObjectInfoCodecDecode(iterator *bufutil.ForwardFrameIterator)  DistributedObjectInfo  {
        // begin frame
        iterator.Next()
        serviceName := bufutil.StringCodecDecode(iterator)
        name := bufutil.StringCodecDecode(iterator)

        bufutil.FastForwardToEndFrame(iterator)

        return DistributedObjectInfo { serviceName, name }
    }