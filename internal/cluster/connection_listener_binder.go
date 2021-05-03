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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type listenerRegistration struct {
	addRequest    *proto.ClientMessage
	removeRequest *proto.ClientMessage
	handler       proto.ClientMessageHandler
}

type ConnectionListenerBinder struct {
	connectionManager *ConnectionManager
	invocationFactory *ConnectionInvocationFactory
	eventDispatcher   *event.DispatchService
	requestCh         chan<- invocation.Invocation
	regs              map[internal.UUID]listenerRegistration
	regsMu            *sync.RWMutex
	smart             bool
	logger            logger.Logger
	connectionCount   int32
}

func NewConnectionListenerBinder(
	connManager *ConnectionManager,
	invocationFactory *ConnectionInvocationFactory,
	requestCh chan<- invocation.Invocation,
	eventDispatcher *event.DispatchService,
	logger logger.Logger,
	smart bool) *ConnectionListenerBinder {
	binder := &ConnectionListenerBinder{
		connectionManager: connManager,
		invocationFactory: invocationFactory,
		eventDispatcher:   eventDispatcher,
		requestCh:         requestCh,
		regs:              map[internal.UUID]listenerRegistration{},
		regsMu:            &sync.RWMutex{},
		logger:            logger,
		smart:             smart,
	}
	eventDispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, binder.handleConnectionOpened)
	eventDispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, binder.handleConnectionClosed)
	return binder
}

func (b *ConnectionListenerBinder) sendAddListenerRequest(request *proto.ClientMessage, conn *Connection, handler proto.ClientMessageHandler) error {
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, handler)
	b.requestCh <- inv
	_, err := inv.Get()
	return err
}

func (b *ConnectionListenerBinder) Add(id internal.UUID, add *proto.ClientMessage, remove *proto.ClientMessage, handler proto.ClientMessageHandler) error {
	b.regsMu.Lock()
	b.regs[id] = listenerRegistration{
		addRequest:    add,
		removeRequest: remove,
		handler:       handler,
	}
	b.regsMu.Unlock()
	for _, conn := range b.connectionManager.ActiveConnections() {
		if err := b.sendAddListenerRequest(add, conn, handler); err != nil {
			return err
		}
	}
	return nil
}

func (b *ConnectionListenerBinder) Remove(id internal.UUID) error {
	b.regsMu.Lock()
	defer b.regsMu.Unlock()
	reg, ok := b.regs[id]
	if !ok {
		return nil
	}
	for _, conn := range b.connectionManager.ActiveConnections() {
		if err := b.sendRemoveListenerRequest(reg.removeRequest, conn); err != nil {
			return err
		}
	}
	return nil
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequest(request *proto.ClientMessage, conn *Connection) error {
	inv := b.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, nil)
	b.requestCh <- inv
	_, err := inv.Get()
	return err
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
			if err := b.sendAddListenerRequest(reg.addRequest, e.Conn, reg.handler); err != nil {
				b.logger.Errorf("adding listener on connection: %d", e.Conn.ConnectionID())
			}
		}
	}
}

func (b *ConnectionListenerBinder) handleConnectionClosed(event event.Event) {
	if _, ok := event.(*ConnectionOpened); ok {
		atomic.AddInt32(&b.connectionCount, -1)
	}
}
