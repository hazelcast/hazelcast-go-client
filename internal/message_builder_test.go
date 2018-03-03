// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"reflect"
	"sync"
	"testing"
)

func TestClientMessageBuilder_OnMessage(t *testing.T) {
	builder := &ClientMessageBuilder{
		incompleteMessages: make(map[int64]*ClientMessage),
	}
	var mu = sync.Mutex{}
	// make this channel blocking to ensure that test wont continue until the builtClientMessage is received
	ch := make(chan *ClientMessage, 0)
	builder.responseChannel = ch
	var builtClientMessage *ClientMessage
	go func() {
		mu.Lock()
		builtClientMessage = <-ch
		mu.Unlock()
	}()

	testString := "testString"
	serverVersion := "3.9"
	expectedClientMessage := ClientAuthenticationEncodeRequest(&testString, &testString, &testString, &testString, false,
		&testString, 1, &serverVersion)
	expectedClientMessage.SetFlags(common.BEGIN_END_FLAG)
	expectedClientMessage.SetCorrelationId(1)
	expectedClientMessage.SetFrameLength(int32(len(expectedClientMessage.Buffer)))

	buffer := expectedClientMessage.Buffer
	payloadSize := uint16(len(buffer)) - expectedClientMessage.DataOffset()

	firstBuffer := append(buffer[0:expectedClientMessage.DataOffset()], buffer[expectedClientMessage.DataOffset():expectedClientMessage.DataOffset()+payloadSize/2]...)
	secondBuffer := append(buffer[0:expectedClientMessage.DataOffset()], buffer[expectedClientMessage.DataOffset()+payloadSize/2:]...)

	firstMessage := NewClientMessage(firstBuffer, 0)
	secondMessage := NewClientMessage(secondBuffer, 0)

	firstMessage.SetFrameLength(int32(len(firstMessage.Buffer)))
	secondMessage.SetFrameLength(int32(len(secondMessage.Buffer)))

	firstMessage.SetFlags(common.BEGIN_FLAG)
	builder.OnMessage(firstMessage)
	secondMessage.SetFlags(common.END_FLAG)
	builder.OnMessage(secondMessage)
	mu.Lock()
	if !reflect.DeepEqual(builtClientMessage.Buffer, expectedClientMessage.Buffer) {
		t.Fatal("message builder has failed")
	}
	mu.Unlock()

}

func TestClientMessageBuilder_OnMessageWithNotFoundCorrelationId(t *testing.T) {
	builder := &ClientMessageBuilder{
		incompleteMessages: make(map[int64]*ClientMessage),
	}
	// make this channel blocking to ensure that test wont continue until the builtClientMessage is received
	ch := make(chan *ClientMessage, 0)
	builder.responseChannel = ch
	msg := NewClientMessage(nil, 40)
	msg.SetCorrelationId(2)
	msg.SetFrameLength(int32(len(msg.Buffer)))
	builder.OnMessage(msg)
}
