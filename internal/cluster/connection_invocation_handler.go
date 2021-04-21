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
	"fmt"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

type ConnectionInvocationHandlerCreationBundle struct {
	ConnectionManager *ConnectionManager
	ClusterService    *ServiceImpl
	Logger            logger.Logger
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
}

type ConnectionInvocationHandler struct {
	connectionManager *ConnectionManager
	clusterService    *ServiceImpl
	smart             int32
	logger            logger.Logger
	cb                *cb.CircuitBreaker
}

func NewConnectionInvocationHandler(bundle ConnectionInvocationHandlerCreationBundle) *ConnectionInvocationHandler {
	bundle.Check()
	// TODO: make circuit breaker configurable
	circuitBreaker := cb.NewCircuitBreaker(
		cb.MaxRetries(3),
		cb.MaxFailureCount(3),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration(attempt) * time.Second
		}))
	return &ConnectionInvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		logger:            bundle.Logger,
		cb:                circuitBreaker,
	}
}

func (h *ConnectionInvocationHandler) Invoke(inv invocation.Invocation) error {
	_, err := h.cb.Try(func(ctx context.Context) (interface{}, error) {
		if h.clusterService.SmartRoutingEnabled() {
			if err := h.invokeSmart(inv); err != nil {
				h.logger.Warnf("invoking non smart since: %s", err.Error())
				return nil, h.invokeNonSmart(inv)
			}
			return nil, nil
		} else {
			return nil, h.invokeNonSmart(inv)
		}
	}).Result()
	return err
}

func (h *ConnectionInvocationHandler) invokeSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok && boundInvocation.Connection() != nil {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	} else if inv.PartitionID() != -1 {
		if conn := h.connectionManager.GetConnectionForPartition(inv.PartitionID()); conn == nil {
			return fmt.Errorf("connection for partition ID %d not found", inv.PartitionID())
		} else {
			return h.sendToConnection(inv, conn)
		}
	} else if inv.Address() != nil {
		return h.sendToAddress(inv, inv.Address())
	} else {
		return h.sendToOwnerAddress(inv)
	}
}

func (h *ConnectionInvocationHandler) invokeNonSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok && boundInvocation.Connection() != nil {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	}
	return h.sendToOwnerAddress(inv)
}

func (h *ConnectionInvocationHandler) sendToConnection(inv invocation.Invocation, conn *Connection) error {
	if sent := conn.send(inv); !sent {
		return hzerror.NewHazelcastIOError("packet is not sent", nil)
	}
	return nil
}

func (h *ConnectionInvocationHandler) sendToAddress(inv invocation.Invocation, addr *pubcluster.AddressImpl) error {
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

func (h *ConnectionInvocationHandler) sendToOwnerAddress(inv invocation.Invocation) error {
	if addr := h.connectionManager.OwnerConnectionAddr(); addr == nil {
		// TODO: change error type
		return hzerror.NewHazelcastIOError("cannot send to owner address: not found", nil)
	} else {
		return h.sendToAddress(inv, addr)
	}
}
