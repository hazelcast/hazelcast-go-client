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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func EncodeHazelcastJsonValue(clientMessage *proto.ClientMessage, jsonValue serialization.JSON) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())

	EncodeString(clientMessage, jsonValue)

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeHazelcastJsonValue(frameIterator *proto.ForwardFrameIterator) serialization.JSON {
	// begin frame
	frameIterator.Next()

	value := serialization.JSON(DecodeString(frameIterator))
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return value
}
