package cluster

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type ViewListenerService struct {
	cs         *Service
	cm         *ConnectionManager
	dispatcher *event.DispatchService
	logger     logger.Logger
	doneCh     chan struct{}
	connID     int64
	state      int32
}

func NewViewListenerService(cs *Service, cm *ConnectionManager, dispatcher *event.DispatchService, logger logger.Logger) *ViewListenerService {
	vs := &ViewListenerService{
		cs:         cs,
		cm:         cm,
		dispatcher: dispatcher,
		logger:     logger,
		doneCh:     make(chan struct{}),
		state:      ready,
	}
	dispatcher.Subscribe(EventConnectionOpened, event.DefaultSubscriptionID, vs.handleConnectionOpened)
	dispatcher.Subscribe(EventConnectionClosed, event.DefaultSubscriptionID, vs.handleConnectionClosed)
	return vs
}

func (vs *ViewListenerService) Stop() {
	if !atomic.CompareAndSwapInt32(&vs.state, ready, stopped) {
		return
	}
	close(vs.doneCh)
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
	// cancel the request when the service is stopped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-vs.doneCh
		cancel()
	}()
	if err := vs.cs.sendMemberListViewRequest(ctx, conn); err != nil {
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
