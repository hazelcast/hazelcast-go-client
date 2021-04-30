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
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type ListenerResponseDecoder func(response *proto.ClientMessage) internal.UUID
type ListenerRemoveMsgMaker func(subscriptionID internal.UUID) *proto.ClientMessage

type serverRegistration struct {
	id            internal.UUIDImpl
	removeRequest *proto.ClientMessage
}
type connRegistration struct {
	conn          *Connection
	client        int64
	server        internal.UUIDImpl
	removeRequest *proto.ClientMessage
}

type messageHandler struct {
	message *proto.ClientMessage
	handler proto.ClientMessageHandler
}

type ConnectionListenerBinderImpl struct {
	connectionManager     *ConnectionManager
	invocationFactory     *ConnectionInvocationFactory
	eventDispatcher       *event.DispatchService
	requestCh             chan<- invocation.Invocation
	connToRegistration    map[int64]map[int64]serverRegistration
	registrationToMessage map[int64]messageHandler
	connToRegistrationMu  *sync.RWMutex
}

func NewConnectionListenerBinderImpl(
	connManager *ConnectionManager,
	invocationFactory *ConnectionInvocationFactory,
	requestCh chan<- invocation.Invocation,
	eventDispatcher *event.DispatchService) *ConnectionListenerBinderImpl {
	binder := &ConnectionListenerBinderImpl{
		connectionManager:     connManager,
		invocationFactory:     invocationFactory,
		eventDispatcher:       eventDispatcher,
		requestCh:             requestCh,
		connToRegistration:    map[int64]map[int64]serverRegistration{},
		registrationToMessage: map[int64]messageHandler{},
		connToRegistrationMu:  &sync.RWMutex{},
	}
	eventDispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, binder.handleConnectionOpened)
	return binder
}

func (b *ConnectionListenerBinderImpl) Add(
	addRequest *proto.ClientMessage,
	clientRegistrationID int64,
	handler proto.ClientMessageHandler,
	responseDecoder ListenerResponseDecoder,
	listenerRemover ListenerRemoveMsgMaker) error {
	connToRegistration := map[int64]connRegistration{}
	for _, conn := range b.connectionManager.ActiveConnections() {
		inv := b.invocationFactory.NewConnectionBoundInvocation(addRequest, -1, nil, conn, handler)
		b.requestCh <- inv
		if response, err := inv.Get(); err != nil {
			return err
		} else {
			serverRegistrationID := responseDecoder(response).(internal.UUIDImpl)
			connToRegistration[conn.connectionID] = connRegistration{
				client:        clientRegistrationID,
				server:        serverRegistrationID,
				removeRequest: listenerRemover(serverRegistrationID),
			}
		}
	}
	// merge connToRegistration to the main one
	b.connToRegistrationMu.Lock()
	defer b.connToRegistrationMu.Unlock()
	for connID, reg := range connToRegistration {
		serverReg := serverRegistration{
			id:            reg.server,
			removeRequest: reg.removeRequest,
		}
		// if the map for connection ID doesn't exist, create it
		if connReg, ok := b.connToRegistration[connID]; ok {
			connReg[reg.client] = serverReg
		} else {
			b.connToRegistration[connID] = map[int64]serverRegistration{
				reg.client: serverReg,
			}
		}
		b.registrationToMessage[reg.client] = messageHandler{
			message: addRequest,
			handler: handler,
		}
	}
	return nil
}

func (b *ConnectionListenerBinderImpl) Remove(
	mapName string,
	clientRegistrationID int64) error {
	activeConns := b.connectionManager.ActiveConnections()
	connToRegistration := []connRegistration{}
	b.connToRegistrationMu.RLock()
	for _, conn := range activeConns {
		if regs, ok := b.connToRegistration[conn.connectionID]; ok {
			if serverReg, ok := regs[clientRegistrationID]; ok {
				connToRegistration = append(connToRegistration, connRegistration{
					conn:          conn,
					client:        clientRegistrationID,
					server:        serverReg.id,
					removeRequest: serverReg.removeRequest,
				})
			}
		}
	}
	b.connToRegistrationMu.RUnlock()
	for _, reg := range connToRegistration {
		inv := b.invocationFactory.NewConnectionBoundInvocation(reg.removeRequest, -1, nil, reg.conn, nil)
		b.requestCh <- inv
		if _, err := inv.Get(); err != nil {
			return err
		}
	}
	b.connToRegistrationMu.Lock()
	defer b.connToRegistrationMu.Unlock()
	for _, reg := range connToRegistration {
		if regs, ok := b.connToRegistration[reg.conn.connectionID]; ok {
			delete(regs, reg.client)
			delete(b.registrationToMessage, reg.client)
		}
	}
	return nil
}

func (b *ConnectionListenerBinderImpl) handleConnectionOpened(event event.Event) {
	b.connToRegistrationMu.RLock()
	defer b.connToRegistrationMu.RUnlock()
	if connectionOpenedEvent, ok := event.(*ConnectionOpened); ok {
		for _, msg := range b.registrationToMessage {
			inv := b.invocationFactory.NewConnectionBoundInvocation(
				msg.message,
				-1,
				nil,
				connectionOpenedEvent.Conn,
				msg.handler)
			b.requestCh <- inv
		}
	}
}
