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
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
)

var errPartitionOwnerNotAssigned = errors.New("partition owner not assigned")

type ConnectionInvocationHandlerCreationBundle struct {
	ConnectionManager *ConnectionManager
	ClusterService    *Service
	Logger            ilogger.Logger
	Config            *pubcluster.Config
}

func (b ConnectionInvocationHandlerCreationBundle) Check() {
	if b.ConnectionManager == nil {
		panic("ConnectionManager is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.Config == nil {
		panic("Config is nil")
	}
}

type ConnectionInvocationHandler struct {
	logger            ilogger.Logger
	connectionManager *ConnectionManager
	clusterService    *Service
	cb                *cb.CircuitBreaker
	smart             bool
}

func NewConnectionInvocationHandler(bundle ConnectionInvocationHandlerCreationBundle) *ConnectionInvocationHandler {
	bundle.Check()
	// TODO: make circuit breaker configurable
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt64),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration(attempt) * time.Second
		}))
	return &ConnectionInvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		logger:            bundle.Logger,
		cb:                cbr,
		smart:             bundle.Config.SmartRouting,
	}
}

func (h *ConnectionInvocationHandler) Invoke(inv invocation.Invocation) error {
	_, err := h.cb.Try(func(ctx context.Context, attempt int) (interface{}, error) {
		if h.smart {
			if err := h.invokeSmart(inv); err != nil {
				if errors.Is(err, errPartitionOwnerNotAssigned) {
					h.logger.Debug(func() string { return fmt.Sprintf("invoking non-smart since: %s", err.Error()) })
					return nil, h.invokeNonSmart(inv)
				}
				return nil, err
			}
			return nil, nil
		} else {
			return nil, h.invokeNonSmart(inv)
		}
	})
	return err
}

func (h *ConnectionInvocationHandler) invokeSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok && boundInvocation.Connection() != nil {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	}
	if inv.PartitionID() != -1 {
		if conn := h.connectionManager.GetConnectionForPartition(inv.PartitionID()); conn != nil {
			return h.sendToConnection(inv, conn)
		}
	}
	if inv.Address() != "" {
		return h.sendToAddress(inv, inv.Address())
	}
	return h.sendToRandomAddress(inv)
}

func (h *ConnectionInvocationHandler) invokeNonSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok && boundInvocation.Connection() != nil {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	}
	return h.sendToRandomAddress(inv)
}

func (h *ConnectionInvocationHandler) sendToConnection(inv invocation.Invocation, conn *Connection) error {
	if sent := conn.send(inv); !sent {
		return hzerrors.NewHazelcastIOError("packet not sent", nil)
	}
	return nil
}

func (h *ConnectionInvocationHandler) sendToAddress(inv invocation.Invocation, addr pubcluster.Address) error {
	conn := h.connectionManager.GetConnectionForAddress(addr)
	if conn == nil {
		if conn = h.connectionManager.RandomConnection(); conn != nil {
			h.logger.Trace(func() string {
				return fmt.Sprintf("address %s not found for invocation, sending to random connection", addr)
			})
		} else {
			h.logger.Trace(func() string {
				return fmt.Sprintf("sending invocation to %s failed, address not found", addr.String())
			})
			return fmt.Errorf("address not found: %s", addr.String())
		}
	}
	return h.sendToConnection(inv, conn)
}

func (h *ConnectionInvocationHandler) sendToRandomAddress(inv invocation.Invocation) error {
	if conn := h.connectionManager.RandomConnection(); conn == nil {
		// TODO: use correct error type
		return hzerrors.NewHazelcastIOError("no connection found", nil)
	} else {
		return h.sendToConnection(inv, conn)
	}
}
