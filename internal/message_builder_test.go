// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

package internal

import (
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v3/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v3/internal/proto/bufutil"
)

func TestClientMessageBuilder_OnMessage(t *testing.T) {
	builder := &clientMessageBuilder{
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	var builtClientMessage *proto.ClientMessage
	builder.handleResponse = func(command interface{}) {
		builtClientMessage = command.(*proto.ClientMessage)
	}

	testString := "testString"
	serverVersion := "3.9"
	expectedClientMessage := proto.ClientAuthenticationEncodeRequest(testString, testString, testString, testString, false,
		testString, 1, serverVersion)
	expectedClientMessage.SetFlags(bufutil.BeginEndFlag)
	expectedClientMessage.SetCorrelationID(1)
	expectedClientMessage.SetFrameLength(int32(len(expectedClientMessage.Buffer)))

	buffer := expectedClientMessage.Buffer
	payloadSize := uint16(len(buffer)) - expectedClientMessage.DataOffset()

	firstBuffer := append(buffer[0:expectedClientMessage.DataOffset()],
		buffer[expectedClientMessage.DataOffset():expectedClientMessage.DataOffset()+payloadSize/2]...)
	secondBuffer := append(buffer[0:expectedClientMessage.DataOffset()],
		buffer[expectedClientMessage.DataOffset()+payloadSize/2:]...)

	firstMessage := proto.NewClientMessage(firstBuffer, 0)
	secondMessage := proto.NewClientMessage(secondBuffer, 0)

	firstMessage.SetFrameLength(int32(len(firstMessage.Buffer)))
	secondMessage.SetFrameLength(int32(len(secondMessage.Buffer)))

	firstMessage.SetFlags(bufutil.BeginFlag)
	builder.onMessage(firstMessage)
	secondMessage.SetFlags(bufutil.EndFlag)
	builder.onMessage(secondMessage)
	if !reflect.DeepEqual(builtClientMessage.Buffer, expectedClientMessage.Buffer) {
		t.Fatal("message builder has failed")
	}

}

func TestClientMessageBuilder_OnMessageWithNotFoundCorrelationID(t *testing.T) {
	builder := &clientMessageBuilder{
		incompleteMessages: make(map[int64]*proto.ClientMessage),
	}
	builder.handleResponse = func(command interface{}) {
	}
	msg := proto.NewClientMessage(nil, 40)
	msg.SetCorrelationID(2)
	msg.SetFrameLength(int32(len(msg.Buffer)))
	builder.onMessage(msg)
}
