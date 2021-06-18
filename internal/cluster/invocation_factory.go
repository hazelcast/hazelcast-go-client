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
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type ConnectionInvocationFactory struct {
	invocationTimeout time.Duration
	nextCorrelationID int64
	redoOperation     bool
}

func NewConnectionInvocationFactory(config *pubcluster.Config) *ConnectionInvocationFactory {
	return &ConnectionInvocationFactory{
		invocationTimeout: config.InvocationTimeout,
		redoOperation:     config.RedoOperation,
	}
}

func (f *ConnectionInvocationFactory) NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) *invocation.Impl {
	message.SetCorrelationID(f.makeCorrelationID())
	return invocation.NewImpl(message, partitionID, "", time.Now().Add(f.invocationTimeout), f.redoOperation)
}

func (f *ConnectionInvocationFactory) NewInvocationOnRandomTarget(message *proto.ClientMessage, handler proto.ClientMessageHandler) *invocation.Impl {
	message.SetCorrelationID(f.makeCorrelationID())
	inv := invocation.NewImpl(message, -1, "", time.Now().Add(f.invocationTimeout), f.redoOperation)
	inv.SetEventHandler(handler)
	return inv
}

func (f *ConnectionInvocationFactory) NewInvocationOnTarget(message *proto.ClientMessage, addr pubcluster.Address) *invocation.Impl {
	message.SetCorrelationID(f.makeCorrelationID())
	inv := invocation.NewImpl(message, -1, addr, time.Now().Add(f.invocationTimeout), f.redoOperation)
	return inv
}

func (f *ConnectionInvocationFactory) NewConnectionBoundInvocation(message *proto.ClientMessage, conn *Connection, handler proto.ClientMessageHandler) *ConnectionBoundInvocation {
	message = message.Copy()
	message.SetCorrelationID(f.makeCorrelationID())
	inv := newConnectionBoundInvocation(message, -1, "", conn, time.Now().Add(f.invocationTimeout), f.redoOperation)
	inv.SetEventHandler(handler)
	return inv
}

func (f *ConnectionInvocationFactory) makeCorrelationID() int64 {
	return atomic.AddInt64(&f.nextCorrelationID, 1)
}
