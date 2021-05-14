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
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type listenerRegistration struct {
	addRequest    *proto.ClientMessage
	removeRequest *proto.ClientMessage
	handler       proto.ClientMessageHandler
	id            types.UUID
}

type ConnectionListenerBinder struct {
	logger            logger.Logger
	connectionManager *ConnectionManager
	invocationFactory *ConnectionInvocationFactory
	eventDispatcher   *event.DispatchService
	removeCh          chan<- int64
	regs              map[types.UUID]listenerRegistration
	correlationIDs    map[types.UUID][]int64
	regsMu            *sync.RWMutex
	requestCh         chan<- invocation.Invocation
	connectionCount   int32
	smart             bool
}

func NewConnectionListenerBinder(
	connManager *ConnectionManager,
	invocationFactory *ConnectionInvocationFactory,
	requestCh chan<- invocation.Invocation,
	removeCh chan<- int64,
	eventDispatcher *event.DispatchService,
	logger logger.Logger,
	smart bool) *ConnectionListenerBinder {
	binder := &ConnectionListenerBinder{
		connectionManager: connManager,
		invocationFactory: invocationFactory,
		eventDispatcher:   eventDispatcher,
		requestCh:         requestCh,
		removeCh:          removeCh,
		regs:              map[types.UUID]listenerRegistration{},
		correlationIDs:    map[types.UUID][]int64{},
		regsMu:            &sync.RWMutex{},
		logger:            logger,
		smart:             smart,
	}
	eventDispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, binder.handleConnectionOpened)
	eventDispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, binder.handleConnectionClosed)
	return binder
}

func (b *ConnectionListenerBinder) Add(ctx context.Context, id types.UUID, add *proto.ClientMessage, remove *proto.ClientMessage, handler proto.ClientMessageHandler) error {
	b.regsMu.Lock()
	b.regs[id] = listenerRegistration{
		addRequest:    add,
		removeRequest: remove,
		handler:       handler,
		id:            id,
	}
	b.regsMu.Unlock()
	corrIDs, err := b.sendAddListenerRequests(ctx, add, handler, b.connectionManager.ActiveConnections()...)
	if err != nil {
		return err
	}
	b.updateCorrelationIDs(id, corrIDs)
	return nil
}

func (b *ConnectionListenerBinder) Remove(ctx context.Context, id types.UUID) error {
	b.regsMu.Lock()
	reg, ok := b.regs[id]
	if !ok {
		b.regsMu.Unlock()
		return nil
	}
	delete(b.regs, id)
	b.removeCorrelationIDs(id)
	b.regsMu.Unlock()
	return b.sendRemoveListenerRequests(ctx, reg.removeRequest, b.connectionManager.ActiveConnections()...)
}

func (b *ConnectionListenerBinder) updateCorrelationIDs(regID types.UUID, correlationIDs []int64) {
	b.regsMu.Lock()
	if ids, ok := b.correlationIDs[regID]; ok {
		b.correlationIDs[regID] = append(ids, correlationIDs...)
	} else {
		b.correlationIDs[regID] = correlationIDs
	}
	b.regsMu.Unlock()
}

func (b *ConnectionListenerBinder) removeCorrelationIDs(regId types.UUID) {
	if ids, ok := b.correlationIDs[regId]; ok {
		delete(b.correlationIDs, regId)
		for _, id := range ids {
			b.removeCh <- id
		}
	}
}

func (b *ConnectionListenerBinder) sendAddListenerRequests(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler, conns ...*Connection) ([]int64, error) {
	if len(conns) == 0 {
		return nil, nil
	}
	if len(conns) == 1 {
		inv, corrID := b.sendAddListenerRequest(request, handler, conns[0])
		_, err := inv.Get()
		return []int64{corrID}, err
	}
	invs := make([]invocation.Invocation, len(conns))
	corrIDs := make([]int64, len(conns))
	for i, conn := range conns {
		inv, corrID := b.sendAddListenerRequest(request, handler, conn)
		invs[i] = inv
		corrIDs[i] = corrID
	}
	for _, inv := range invs {
		if _, err := inv.GetWithContext(ctx); err != nil {
			return nil, err
		}
	}
	return corrIDs, nil
}

func (b *ConnectionListenerBinder) sendAddListenerRequest(
	request *proto.ClientMessage,
	handler proto.ClientMessageHandler,
	conn *Connection) (invocation.Invocation, int64) {
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, handler)
	correlationID := inv.Request().CorrelationID()
	b.requestCh <- inv
	return inv, correlationID
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequests(ctx context.Context, request *proto.ClientMessage, conns ...*Connection) error {
	if len(conns) == 0 {
		return nil
	}
	if len(conns) == 1 {
		inv := b.sendRemoveListenerRequest(request, conns[0])
		_, err := inv.Get()
		return err
	}
	invs := make([]invocation.Invocation, len(conns))
	for i, conn := range conns {
		invs[i] = b.sendRemoveListenerRequest(request, conn)
	}
	for _, inv := range invs {
		if _, err := inv.GetWithContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequest(request *proto.ClientMessage, conn *Connection) invocation.Invocation {
	b.logger.Trace(func() string {
		return fmt.Sprintf("%d: removing listener", conn.connectionID)
	})
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, nil)
	b.requestCh <- inv
	return inv
}

func (b *ConnectionListenerBinder) handleConnectionOpened(event event.Event) {
	if e, ok := event.(*ConnectionOpened); ok {
		connectionCount := atomic.AddInt32(&b.connectionCount, 1)
		b.regsMu.RLock()
		defer b.regsMu.RUnlock()
		if !b.smart && connectionCount > 0 {
			// do not register new connections in non-smart mode
			return
		}
		for regID, reg := range b.regs {
			b.logger.Debug(func() string {
				return fmt.Sprintf("%d: adding listener %s (new connection)", e.Conn.connectionID, regID.String())
			})
			if corrIDs, err := b.sendAddListenerRequests(context.Background(), reg.addRequest, reg.handler, e.Conn); err != nil {
				b.logger.Errorf("adding listener on connection: %d", e.Conn.ConnectionID())
			} else {
				b.updateCorrelationIDs(regID, corrIDs)
			}
		}
	}
}

func (b *ConnectionListenerBinder) handleConnectionClosed(event event.Event) {
	if _, ok := event.(*ConnectionOpened); ok {
		atomic.AddInt32(&b.connectionCount, -1)
	}
}
