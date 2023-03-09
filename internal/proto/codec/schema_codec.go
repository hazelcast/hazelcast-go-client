/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

func EncodeSchema(clientMessage *proto.ClientMessage, schema *iserialization.Schema) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())

	EncodeString(clientMessage, schema.TypeName)
	EncodeListMultiFrameForFieldDescriptor(clientMessage, schema.FieldDefinitions())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func DecodeSchema(frameIterator *proto.ForwardFrameIterator) *iserialization.Schema {
	// begin frame
	frameIterator.Next()

	typeName := DecodeString(frameIterator)
	fields := DecodeListMultiFrameForFieldDescriptor(frameIterator)
	FastForwardToEndFrame(frameIterator)

	return iserialization.NewSchema(typeName, fields)
}
