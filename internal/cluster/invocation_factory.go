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
}

func NewConnectionInvocationFactory(invocationTimeout time.Duration) *ConnectionInvocationFactory {
	return &ConnectionInvocationFactory{
		invocationTimeout: invocationTimeout,
	}
}

func (f *ConnectionInvocationFactory) NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) invocation.Invocation {
	// TODO: remove this when message pool is implemented
	message = message.Copy()
	message.SetCorrelationID(f.makeCorrelationID())
	return invocation.NewImpl(message, partitionID, nil, f.invocationTimeout)
}

func (f *ConnectionInvocationFactory) NewInvocationOnRandomTarget(message *proto.ClientMessage, handler proto.ClientMessageHandler) invocation.Invocation {
	// TODO: remove this when message pool is implemented
	message = message.Copy()
	message.SetCorrelationID(f.makeCorrelationID())
	inv := invocation.NewImpl(message, -1, nil, f.invocationTimeout)
	inv.SetEventHandler(handler)
	return inv
}

func (f *ConnectionInvocationFactory) NewInvocationOnTarget(message *proto.ClientMessage, address *pubcluster.AddressImpl) invocation.Invocation {
	// TODO: remove this when message pool is implemented
	message = message.Copy()
	message.SetCorrelationID(f.makeCorrelationID())
	return invocation.NewImpl(message, -1, address, f.invocationTimeout)
}

func (f *ConnectionInvocationFactory) NewConnectionBoundInvocation(message *proto.ClientMessage, partitionID int32, address *pubcluster.AddressImpl,
	connection *Connection, timeout time.Duration, handler proto.ClientMessageHandler) *ConnectionBoundInvocation {
	// TODO: remove this when message pool is implemented
	message = message.Copy()
	message.SetCorrelationID(f.makeCorrelationID())
	inv := newConnectionBoundInvocation(message, partitionID, address, connection, timeout)
	inv.SetEventHandler(handler)
	return inv
}

func (f *ConnectionInvocationFactory) makeCorrelationID() int64 {
	return atomic.AddInt64(&f.nextCorrelationID, 1)
}
