package cluster

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type ViewListenerService struct {
	connID     int64 // This field should be at the top: https://pkg.go.dev/sync/atomic#pkg-note-BUG
	cs         *Service
	cm         *ConnectionManager
	dispatcher *event.DispatchService
	logger     logger.Logger
}

func NewViewListenerService(cs *Service, cm *ConnectionManager, dispatcher *event.DispatchService, logger logger.Logger) *ViewListenerService {
	vs := &ViewListenerService{
		cs:         cs,
		cm:         cm,
		dispatcher: dispatcher,
		logger:     logger,
	}
	dispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, vs.handleConnectionOpened)
	dispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, vs.handleConnectionClosed)
	return vs
}

func (vs *ViewListenerService) handleConnectionOpened(event event.Event) {
	vs.logger.Trace(func() string { return "cluster.ViewListenerService.handleConnectionOpened" })
	if e, ok := event.(*ConnectionOpened); ok {
		vs.tryRegister(e.Conn)
	}
}

func (vs *ViewListenerService) handleConnectionClosed(event event.Event) {
	vs.logger.Trace(func() string { return "cluster.ViewListenerService.handleConnectionClosed" })
	if e, ok := event.(*ConnectionClosed); ok {
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
