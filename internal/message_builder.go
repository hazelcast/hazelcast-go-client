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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type ClientMessageBuilder struct {
	incompleteMessages map[int64]*ClientMessage
	responseChannel    chan *ClientMessage
}

func (cmb *ClientMessageBuilder) OnMessage(msg *ClientMessage) {
	if msg.HasFlags(common.BEGIN_END_FLAG) > 0 {
		cmb.responseChannel <- msg
	} else if msg.HasFlags(common.BEGIN_FLAG) > 0 {
		cmb.incompleteMessages[msg.CorrelationId()] = msg
	} else {
		message, found := cmb.incompleteMessages[msg.CorrelationId()]
		if !found {
			return
		}
		message.Accumulate(msg)
		if msg.HasFlags(common.END_FLAG) > 0 {
			message.AddFlags(common.BEGIN_END_FLAG)
			cmb.responseChannel <- message
			delete(cmb.incompleteMessages, msg.CorrelationId())
		}
	}
}
