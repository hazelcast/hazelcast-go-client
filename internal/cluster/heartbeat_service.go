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
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type HeartbeatService struct {
	cm         *ConnectionManager
	invFactory *ConnectionInvocationFactory
	invService *invocation.Service
	doneCh     chan struct{}
	logger     logger.LogAdaptor
	interval   time.Duration
	timeout    time.Duration
	state      int32
}

func NewHeartbeatService(cm *ConnectionManager, f *ConnectionInvocationFactory, invService *invocation.Service, logger logger.LogAdaptor, interval, timeout time.Duration) *HeartbeatService {
	return &HeartbeatService{
		cm:         cm,
		invFactory: f,
		invService: invService,
		logger:     logger,
		doneCh:     make(chan struct{}),
		interval:   interval,
		timeout:    timeout,
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
	ticker := time.NewTicker(hs.interval)
	defer ticker.Stop()
	for {
		select {
		case <-hs.doneCh:
			return
		case <-ticker.C:
			for _, conn := range hs.cm.ActiveConnections() {
				hs.sendHeartbeat(conn, hs.timeout, hs.interval)
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
