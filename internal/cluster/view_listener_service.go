package cluster

import (
	"context"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
)

type ViewListenerService struct {
	cs         *Service
	cm         *ConnectionManager
	dispatcher *event.DispatchService
	connID     int64
}

func NewViewListenerService(cs *Service, cm *ConnectionManager, dispatcher *event.DispatchService) *ViewListenerService {
	vs := &ViewListenerService{
		cs:         cs,
		cm:         cm,
		dispatcher: dispatcher,
		connID:     -1,
	}
	dispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, vs.handleConnectionOpened)
	dispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, vs.handleConnectionClosed)
	return vs
}

func (vs *ViewListenerService) handleConnectionOpened(event event.Event) {
	if e, ok := event.(*ConnectionOpened); ok {
		vs.tryRegister(e.Conn)
	}
}

func (vs *ViewListenerService) handleConnectionClosed(event event.Event) {
	if e, ok := event.(*ConnectionClosed); ok {
		vs.tryReregisterToRandomConnection(e.Conn)
	}
}

func (vs *ViewListenerService) tryRegister(conn *Connection) {
	if !atomic.CompareAndSwapInt64(&vs.connID, -1, conn.connectionID) {
		return
	}
	if err := vs.cs.sendMemberListViewRequest(context.Background(), conn); err != nil {
		vs.tryReregisterToRandomConnection(conn)
	}
}

func (vs *ViewListenerService) tryReregisterToRandomConnection(oldConn *Connection) {
	if !atomic.CompareAndSwapInt64(&vs.connID, oldConn.connectionID, -1) {
		return
	}
	if conn := vs.cm.RandomConnection(); conn != nil {
		vs.tryRegister(conn)
	}
}
