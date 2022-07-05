/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

var viewListenerConnectionEventSubID = event.NextSubscriptionID()

type ViewListenerService struct {
	connID     int64 // This field should be at the top: https://pkg.go.dev/sync/atomic#pkg-note-BUG
	cs         *Service
	cm         *ConnectionManager
	dispatcher *event.DispatchService
	logger     logger.LogAdaptor
}

func NewViewListenerService(cs *Service, cm *ConnectionManager, dispatcher *event.DispatchService, logger logger.LogAdaptor) *ViewListenerService {
	vs := &ViewListenerService{
		cs:         cs,
		cm:         cm,
		dispatcher: dispatcher,
		logger:     logger,
	}
	dispatcher.Subscribe(EventConnection, viewListenerConnectionEventSubID, vs.handleConnectionEvent)
	return vs
}

func (vs *ViewListenerService) handleConnectionEvent(event event.Event) {
	vs.logger.Trace(func() string { return fmt.Sprintf("cluster.ViewListenerService.handleConnectionEvent %v", event) })
	e := event.(*ConnectionStateChangedEvent)
	if e.state == ConnectionStateOpened {
		vs.tryRegister(e.Conn)
	} else {
		vs.tryReregisterToRandomConnection(e.Conn)
	}
}

func (vs *ViewListenerService) tryRegister(conn *Connection) {
	vs.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ViewListenerService.tryRegister (status: %d): %d", atomic.LoadInt32(&conn.status), conn.connectionID)
	})
	if !atomic.CompareAndSwapInt64(&vs.connID, 0, conn.connectionID) {
		return
	}
	if err := vs.cs.sendMemberListViewRequest(context.Background(), conn); err != nil {
		vs.tryReregisterToRandomConnection(conn)
	}
}

func (vs *ViewListenerService) tryReregisterToRandomConnection(oldConn *Connection) {
	vs.logger.Trace(func() string {
		return fmt.Sprintf("cluster.ViewListenerService.tryReRegister: %d", oldConn.connectionID)
	})
	if !atomic.CompareAndSwapInt64(&vs.connID, oldConn.connectionID, 0) {
		return
	}
	if conn := vs.cm.RandomConnection(); conn != nil {
		vs.tryRegister(conn)
	}
}
