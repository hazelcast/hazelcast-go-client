/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

var listenerBinderConnectionEventSubID = event.NextSubscriptionID()

type listenerRegistration struct {
	addRequest    *proto.ClientMessage
	removeRequest *proto.ClientMessage
	handler       proto.ClientMessageHandler
	id            types.UUID
}

type ConnectionListenerBinder struct {
	logger                logger.LogAdaptor
	connectionManager     *ConnectionManager
	invocationFactory     *ConnectionInvocationFactory
	eventDispatcher       *event.DispatchService
	invocationService     *invocation.Service
	regs                  map[types.UUID]listenerRegistration
	correlationIDs        map[types.UUID][]int64
	subscriptionToMembers map[types.UUID]map[types.UUID]struct{}
	memberSubscriptions   map[types.UUID][]types.UUID
	regsMu                *sync.RWMutex
	connectionCount       int32
	smart                 bool
}

func NewConnectionListenerBinder(
	connManager *ConnectionManager,
	invocationService *invocation.Service,
	invocationFactory *ConnectionInvocationFactory,
	eventDispatcher *event.DispatchService,
	logger logger.LogAdaptor,
	smart bool) *ConnectionListenerBinder {
	binder := &ConnectionListenerBinder{
		connectionManager:     connManager,
		invocationService:     invocationService,
		invocationFactory:     invocationFactory,
		eventDispatcher:       eventDispatcher,
		regs:                  map[types.UUID]listenerRegistration{},
		correlationIDs:        map[types.UUID][]int64{},
		subscriptionToMembers: map[types.UUID]map[types.UUID]struct{}{},
		memberSubscriptions:   map[types.UUID][]types.UUID{},
		regsMu:                &sync.RWMutex{},
		logger:                logger,
		smart:                 smart,
	}
	eventDispatcher.Subscribe(EventConnection, listenerBinderConnectionEventSubID, binder.handleConnectionEvent)
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
	conns := b.connectionManager.ActiveConnections()
	conns = FilterConns(conns, func(conn *Connection) bool {
		return !b.connExists(conn, id)
	})
	b.logger.Trace(func() string {
		return fmt.Sprintf("adding listener %s:\nconns: %v,\nregs: %v", id, conns, b.regs)
	})
	corrIDs, err := b.sendAddListenerRequests(ctx, add, handler, conns...)
	if err != nil {
		return err
	}
	b.updateCorrelationIDs(id, corrIDs)
	for _, conn := range conns {
		b.addSubscriptionToMember(id, conn.MemberUUID())
	}
	return nil
}

func (b *ConnectionListenerBinder) Remove(ctx context.Context, id types.UUID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.regsMu.Lock()
	defer b.regsMu.Unlock()
	reg, ok := b.regs[id]
	if !ok {
		return nil
	}
	delete(b.regs, id)
	b.removeCorrelationIDs(id)
	conns := b.connectionManager.ActiveConnections()
	b.logger.Trace(func() string {
		return fmt.Sprintf("removing listener %s:\nconns: %v,\nregs: %v", id, conns, b.regs)
	})
	for _, conn := range conns {
		b.removeMemberSubscriptions(conn.MemberUUID())
	}
	return b.sendRemoveListenerRequests(ctx, reg.removeRequest, conns...)
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
	req := inv.Request()
	cid, err := b.connectionManager.invoker.CB().TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			req = req.Copy()
		}
		if err := b.invocationService.SendRequest(ctx, inv); err != nil {
			return nil, err
		}
		return req.CorrelationID(), nil
	})
	if err != nil {
		return nil, 0, err
	}
	return inv, cid.(int64), nil
}

func (b *ConnectionListenerBinder) sendRemoveListenerRequests(ctx context.Context, request *proto.ClientMessage, conns ...*Connection) error {
	if len(conns) == 0 {
		return nil
	}
	now := time.Now()
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
	req := inv.Request()
	_, err := b.connectionManager.invoker.CB().TryContext(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			req = req.Copy()
		}
		return nil, b.invocationService.SendRequest(ctx, inv)
	})
	if err != nil {
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
		if b.connExists(e.Conn, regID) {
			b.logger.Trace(func() string {
				return fmt.Sprintf("listener %s already subscribed to member %s", regID, e.Conn.MemberUUID())
			})
			continue
		}
		b.logger.Debug(func() string {
			return fmt.Sprintf("adding listener %s:\nconns: [%v],\nregs: %v, source: handleConnectionOpened", regID, e.Conn, b.regs)
		})
		corrIDs, err := b.sendAddListenerRequests(context.Background(), reg.addRequest, reg.handler, e.Conn)
		if err != nil {
			b.logger.Errorf("adding listener on connection: %d", e.Conn.ConnectionID())
			return
		}
		b.updateCorrelationIDs(regID, corrIDs)
		b.addSubscriptionToMember(regID, e.Conn.MemberUUID())
	}
}

func (b *ConnectionListenerBinder) handleConnectionClosed(e *ConnectionStateChangedEvent) {
	atomic.AddInt32(&b.connectionCount, -1)
	b.regsMu.Lock()
	b.removeMemberSubscriptions(e.Conn.MemberUUID())
	b.regsMu.Unlock()
}

func (b *ConnectionListenerBinder) connExists(conn *Connection, subID types.UUID) bool {
	mems, found := b.subscriptionToMembers[subID]
	if !found {
		return false
	}
	_, found = mems[conn.MemberUUID()]
	return found
}

func (b *ConnectionListenerBinder) addSubscriptionToMember(subID types.UUID, memberUUID types.UUID) {
	// this method should be called under lock
	mems, found := b.subscriptionToMembers[subID]
	if !found {
		mems = make(map[types.UUID]struct{})
		b.subscriptionToMembers[subID] = mems
	}
	mems[memberUUID] = struct{}{}
	b.memberSubscriptions[memberUUID] = append(b.memberSubscriptions[memberUUID], subID)
}

func (b *ConnectionListenerBinder) removeMemberSubscriptions(memberUUID types.UUID) {
	// this method should be called under lock
	subs, found := b.memberSubscriptions[memberUUID]
	if !found {
		return
	}
	for _, sub := range subs {
		delete(b.subscriptionToMembers, sub)
	}
	delete(b.memberSubscriptions, memberUUID)
}
