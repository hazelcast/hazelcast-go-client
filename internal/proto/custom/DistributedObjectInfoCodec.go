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

//@Generated("64ebc78d52fcaa1e36bb4ec32bb7dfe2")
const (
)

func DistributedObjectInfoEncode(clientMessage bufutil.ClientMessagex, distributedObjectInfo DistributedObjectInfo) {
        clientMessage.Add(bufutil.BeginFrame)
        StringCodec.encode(clientMessage, distributedObjectInfo.serviceName)
        StringCodec.encode(clientMessage, distributedObjectInfo.name)

        clientMessage.Add(bufutil.EndFrame)
    }
func DistributedObjectInfoDecode(iterator bufutil.ClientMessagex)  *DistributedObjectInfo  {
        // begin frame
        iterator.Next()
        serviceName := StringCodec.decode(iterator)
        name := StringCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &DistributedObjectInfo { serviceName, name }
    }