package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	logger2 "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type HeartbeatService struct {
	cm         *ConnectionManager
	invFactory *ConnectionInvocationFactory
	invService *invocation.Service
	doneCh     chan struct{}
	logger     logger2.Logger
	state      int32
}

func NewHeartbeatService(cm *ConnectionManager, f *ConnectionInvocationFactory, invService *invocation.Service, logger logger2.Logger) *HeartbeatService {
	return &HeartbeatService{
		cm:         cm,
		invFactory: f,
		invService: invService,
		logger:     logger,
		doneCh:     make(chan struct{}),
		state:      ready,
	}
}

func (hs *HeartbeatService) Start() {
	go hs.checkConnections()
}

func (hs *HeartbeatService) Stop() {
	if atomic.CompareAndSwapInt32(&hs.state, ready, stopped) {
		close(hs.doneCh)
	}
}

func (hs *HeartbeatService) checkConnections() {
	ticker := time.NewTicker(time.Duration(hs.cm.clusterConfig.HeartbeatInterval))
	defer ticker.Stop()
	for {
		select {
		case <-hs.doneCh:
			return
		case <-ticker.C:
			timeout := time.Duration(hs.cm.clusterConfig.HeartbeatTimeout)
			interval := time.Duration(hs.cm.clusterConfig.HeartbeatInterval)
			for _, conn := range hs.cm.ActiveConnections() {
				hs.sendHeartbeat(conn, timeout, interval)
			}
		}
	}
}

func (hs *HeartbeatService) sendHeartbeat(conn *Connection, timeout, interval time.Duration) {
	if !conn.isAlive() {
		return
	}
	now := time.Now()
	// check whether the connection had a heartbeat before
	if conn.lastRead.Load().(time.Time).Before(now.Add(-timeout)) {
		hs.logger.Warnf("heartbeat failed for connection: %s", conn.String())
		conn.close(fmt.Errorf("heartbeat timed out: %w", hzerrors.ErrTargetDisconnected))
		return
	}
	// send a ping to the member if a write wasn't observed recently
	if conn.lastWrite.Load().(time.Time).After(now.Add(-interval)) {
		// there was a recent write, no need to send a ping
		return
	}
	hs.logger.Trace(func() string {
		return fmt.Sprintf("heartbeat: %s", conn.String())
	})
	request := codec.EncodeClientPingRequest()
	inv := hs.invFactory.NewConnectionBoundInvocation(request, conn, nil, time.Now())
	if err := hs.invService.SendUrgentRequest(context.Background(), inv); err != nil {
		hs.logger.Debug(func() string {
			return fmt.Sprintf("Failed to send the heartbeat request: %s", err.Error())
		})
	}
}
