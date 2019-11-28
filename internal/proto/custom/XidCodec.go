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

type Xid struct {
formatId int
globalTransactionId []byte
branchQualifier []byte
}

//@Generated("9bdb56a2d12c88d1fcc78b5990d60c9b")
const (
    XidFormatIdFieldOffset = 0
    XidInitialFrameSize = XidFormatIdFieldOffset + bufutil.IntSizeInBytes
)

func XidEncode(clientMessage bufutil.ClientMessagex, xid Xid) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, XidInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, XidFormatIdFieldOffset, xid.formatId)
        clientMessage.Add(initialFrame)
        ByteArrayCodec.encode(clientMessage, xid.globalTransactionId)
        ByteArrayCodec.encode(clientMessage, xid.branchQualifier)

        clientMessage.Add(bufutil.EndFrame)
    }
func XidDecode(iterator bufutil.ClientMessagex)  *SerializableXID  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        formatId := bufutil.DecodeInt(initialFrame.Content, XidFormatIdFieldOffset)
        globalTransactionId := ByteArrayCodec.decode(iterator)
        branchQualifier := ByteArrayCodec.decode(iterator)

        bufutil.fastForwardToEndFrame(iterator)

        return &SerializableXID { formatId, globalTransactionId, branchQualifier }
    }