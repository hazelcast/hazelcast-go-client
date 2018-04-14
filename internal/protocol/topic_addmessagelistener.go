// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/common"
)

func TopicAddMessageListenerCalculateSize(name *string, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += common.BoolSizeInBytes
	return dataSize
}

func TopicAddMessageListenerEncodeRequest(name *string, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, TopicAddMessageListenerCalculateSize(name, localOnly))
	clientMessage.SetMessageType(topicAddMessageListener)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendBool(localOnly)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func TopicAddMessageListenerDecodeResponse(clientMessage *ClientMessage) func() (response *string) {
	// Decode response from client message
	return func() (response *string) {
		response = clientMessage.ReadString()
		return
	}
}

type TopicAddMessageListenerHandleEventTopicFunc func(*serialization.Data, int64, *string)

func TopicAddMessageListenerEventTopicDecode(clientMessage *ClientMessage) (item *serialization.Data, publishTime int64, uuid *string) {
	item = clientMessage.ReadData()
	publishTime = clientMessage.ReadInt64()
	uuid = clientMessage.ReadString()
	return
}

func TopicAddMessageListenerHandle(clientMessage *ClientMessage,
	handleEventTopic TopicAddMessageListenerHandleEventTopicFunc) {
	// Event handler
	messageType := clientMessage.MessageType()
	if messageType == common.EventTopic && handleEventTopic != nil {
		handleEventTopic(TopicAddMessageListenerEventTopicDecode(clientMessage))
	}
}
