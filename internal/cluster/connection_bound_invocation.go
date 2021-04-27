/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package cluster

import (
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type ConnectionBoundInvocation struct {
	*invocation.Impl
	boundConnection *Connection
}

func newConnectionBoundInvocation(clientMessage *proto.ClientMessage, partitionID int32, address *pubcluster.AddressImpl,
	connection *Connection, deadline time.Time) *ConnectionBoundInvocation {
	return &ConnectionBoundInvocation{
		Impl:            invocation.NewImpl(clientMessage, partitionID, address, deadline),
		boundConnection: connection,
	}
}

func (i *ConnectionBoundInvocation) Connection() *Connection {
	return i.boundConnection
}

func (i *ConnectionBoundInvocation) SetEventHandler(handler proto.ClientMessageHandler) {
	i.Impl.SetEventHandler(handler)
}
