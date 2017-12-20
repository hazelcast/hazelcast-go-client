// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"testing"
)

func TestClientMessageBuilder_OnMessage(t *testing.T) {
	builder := &ClientMessageBuilder{
		incompleteMessages: make(map[int64]*ClientMessage),
	}
	ch := make(chan *ClientMessage)
	builder.responseChannel = ch
	go func() {
		<-ch
	}()

	msg := NewClientMessage(nil, 40)
	msg.SetFlags(common.BEGIN_FLAG)
	msg.SetCorrelationId(1)
	msg.SetFrameLength(int32(len(msg.Buffer)))
	builder.OnMessage(msg)

	msg = NewClientMessage(nil, 40)
	msg.SetCorrelationId(1)
	msg.SetFrameLength(int32(len(msg.Buffer)))
	builder.OnMessage(msg)
	msg.SetFlags(common.END_FLAG)
	builder.OnMessage(msg)

	msg = NewClientMessage(nil, 40)
	msg.SetCorrelationId(2)
	msg.SetFrameLength(int32(len(msg.Buffer)))
	builder.OnMessage(msg)
}
