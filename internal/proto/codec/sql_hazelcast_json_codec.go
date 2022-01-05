/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
)

func EncodeHazelcastJsonValue(clientMessage *proto.ClientMessage, hazelcastJsonValue isql.HazelcastJSON) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())

	EncodeString(clientMessage, hazelcastJsonValue)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeHazelcastJsonValue(frameIterator *proto.ForwardFrameIterator) *isql.HazelcastJSON {
	// begin frame
	frameIterator.Next()

	value := isql.HazelcastJSON(DecodeString(frameIterator))
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return &value
}
