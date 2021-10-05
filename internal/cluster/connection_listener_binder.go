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
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/logger"
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
	invocationService *invocation.Service
	regs              map[types.UUID]listenerRegistration
	correlationIDs    map[types.UUID][]int64
	regsMu            *sync.RWMutex
	connectionCount   int32
	smart             bool
}

func NewConnectionListenerBinder(
	connManager *ConnectionManager,
	invocationService *invocation.Service,
	invocationFactory *ConnectionInvocationFactory,
	eventDispatcher *event.DispatchService,
	logger logger.Logger,
	smart bool) *ConnectionListenerBinder {
	binder := &ConnectionListenerBinder{
		connectionManager: connManager,
		invocationService: invocationService,
		invocationFactory: invocationFactory,
		eventDispatcher:   eventDispatcher,
		regs:              map[types.UUID]listenerRegistration{},
		correlationIDs:    map[types.UUID][]int64{},
		regsMu:            &sync.RWMutex{},
		logger:            logger,
		smart:             smart,
	}
	eventDispatcher.Subscribe(EventConnection, event.DefaultSubscriptionID, binder.handleConnectionEvent)
	return binder
}

func (b *ConnectionListenerBinder) Add(ctx context.Context, id types.UUID, add *proto.ClientMessage, remove *proto.ClientMessage, handler proto.ClientMessageHandler) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.regsMu.Lock()
	defer b.regsMu.Unlock()
	b.regs[id] = listenerRegistration{
		addRequest:    add,
		removeRequest: remove,
		handler:       handler,
		id:            id,
	}
	corrIDs, err := b.sendAddListenerRequests(ctx, add, handler, b.connectionManager.ActiveConnections()...)
	if err != nil {
		return err
	}
	b.updateCorrelationIDs(id, corrIDs)
	return nil
}

func (b *ConnectionListenerBinder) Remove(ctx context.Context, id types.UUID) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
	if ids, ok := b.correlationIDs[regID]; ok {
		b.correlationIDs[regID] = append(ids, correlationIDs...)
	} else {
		b.correlationIDs[regID] = correlationIDs
	}
}

func (b *ConnectionListenerBinder) removeCorrelationIDs(regId types.UUID) {
	if ids, ok := b.correlationIDs[regId]; ok {
		delete(b.correlationIDs, regId)
		for _, id := range ids {
			if err := b.invocationService.Remove(id); err != nil {
				b.logger.Debug(func() string {
					return fmt.Sprintf("removing listener: %s", err.Error())
				})
			}
		}
	}
}

func (b *ConnectionListenerBinder) sendAddListenerRequests(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler, conns ...*Connection) ([]int64, error) {
	if len(conns) == 0 {
		return nil, nil
	}
	now := time.Now()
	if len(conns) == 1 {
		inv, corrID, err := b.sendAddListenerRequest(ctx, request, handler, conns[0], now)
		if err != nil {
			return nil, err
		}
		_, err = inv.GetWithContext(ctx)
		return []int64{corrID}, err
	}
	invs := make([]invocation.Invocation, len(conns))
	corrIDs := make([]int64, len(conns))
	for i, conn := range conns {
		inv, corrID, err := b.sendAddListenerRequest(ctx, request, handler, conn, now)
		if err != nil {
			return nil, err
		}
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

func (b *ConnectionListenerBinder) sendAddListenerRequest(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler, conn *Connection, start time.Time) (invocation.Invocation, int64, error) {
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, conn, handler, start)
	if err := b.invocationService.SendRequest(ctx, inv); err != nil {
		return nil, 0, err
	}
	correlationID := inv.Request().CorrelationID()
	return inv, correlationID, nil
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequests(ctx context.Context, request *proto.ClientMessage, conns ...*Connection) error {
	if len(conns) == 0 {
		return nil
	}
	now := time.Now()
	if len(conns) == 1 {
		inv, err := b.sendRemoveListenerRequest(ctx, request, conns[0], now)
		if err != nil {
			return err
		}
		_, err = inv.GetWithContext(ctx)
		return err
	}
	invs := make([]invocation.Invocation, len(conns))
	for i, conn := range conns {
		inv, err := b.sendRemoveListenerRequest(ctx, request, conn, now)
		if err != nil {
			return err
		}
		invs[i] = inv
	}
	for _, inv := range invs {
		if _, err := inv.GetWithContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequest(ctx context.Context, request *proto.ClientMessage, conn *Connection, start time.Time) (invocation.Invocation, error) {
	b.logger.Trace(func() string {
		return fmt.Sprintf("%d: removing listener", conn.connectionID)
	})
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, conn, nil, start)
	if err := b.invocationService.SendRequest(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

func (b *ConnectionListenerBinder) handleConnectionEvent(event event.Event) {
	e := event.(*ConnectionStateChangedEvent)
	if e.state == ConnectionStateOpened {
		b.handleConnectionOpened(e)
	} else {
		b.handleConnectionClosed(e)
	}
}

func (b *ConnectionListenerBinder) handleConnectionOpened(e *ConnectionStateChangedEvent) {
	connCount := atomic.AddInt32(&b.connectionCount, 1)
	if !b.smart && connCount > 0 {
		// do not register new connections in non-smart mode
		return
	}
	b.regsMu.Lock()
	defer b.regsMu.Unlock()
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

func (b *ConnectionListenerBinder) handleConnectionClosed(_ *ConnectionStateChangedEvent) {
	atomic.AddInt32(&b.connectionCount, -1)
}
