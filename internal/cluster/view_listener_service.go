package cluster

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

// This is a separate struct because the field should be at the top: https://pkg.go.dev/sync/atomic#pkg-note-BUG
// And we don't want to suppress files on fieldAlignment check.
type listenerAtomics struct {
	connID int64
}

type ViewListenerService struct {
	cs         *Service
	cm         *ConnectionManager
	dispatcher *event.DispatchService
	logger     logger.Logger
	atomics    *listenerAtomics
}

func NewViewListenerService(cs *Service, cm *ConnectionManager, dispatcher *event.DispatchService, logger logger.Logger) *ViewListenerService {
	vs := &ViewListenerService{
		cs:         cs,
		cm:         cm,
		dispatcher: dispatcher,
		logger:     logger,
		atomics:    &listenerAtomics{},
	}
	dispatcher.Subscribe(EventConnection, vs.handleConnectionEvent)
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
	if !atomic.CompareAndSwapInt64(&vs.atomics.connID, 0, conn.connectionID) {
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
	if !atomic.CompareAndSwapInt64(&vs.atomics.connID, oldConn.connectionID, 0) {
		return
	}
	if conn := vs.cm.RandomConnection(); conn != nil {
		vs.tryRegister(conn)
	}
}
