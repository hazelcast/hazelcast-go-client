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

type PredicateConfigHolder struct {
className string
sql string
implementation serialization.Data
}

//@Generated("84fd401b7349250b5fe46e5f916af28c")
const (
)

func PredicateConfigHolderEncode(clientMessage bufutil.ClientMessagex, predicateConfigHolder PredicateConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.className, String.encode)
        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.sql, String.encode)
        CodecUtil.encodeNullable(clientMessage, predicateConfigHolder.implementation, Data.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func PredicateConfigHolderDecode(iterator bufutil.ClientMessagex)  *PredicateConfigHolder  {
        // begin frame
        iterator.Next()
        className := CodecUtil.decodeNullable(iterator, String.decode)
        sql := CodecUtil.decodeNullable(iterator, String.decode)
        implementation := CodecUtil.decodeNullable(iterator, Data.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &PredicateConfigHolder { className, sql, implementation }
    }