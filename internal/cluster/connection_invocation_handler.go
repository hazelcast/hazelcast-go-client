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
	"errors"
	"fmt"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

var errPartitionOwnerNotAssigned = errors.New("partition owner not assigned")

type ConnectionInvocationHandlerCreationBundle struct {
	ConnectionManager *ConnectionManager
	ClusterService    *Service
	Logger            logger.Logger
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
	logger            logger.Logger
	connectionManager *ConnectionManager
	clusterService    *Service
	smart             bool
}

func NewConnectionInvocationHandler(bundle ConnectionInvocationHandlerCreationBundle) *ConnectionInvocationHandler {
	bundle.Check()
	return &ConnectionInvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		logger:            bundle.Logger,
		smart:             !bundle.Config.Unisocket,
	}
}

func (h *ConnectionInvocationHandler) Invoke(inv invocation.Invocation) (int64, error) {
	if h.smart {
		groupID, err := h.invokeSmart(inv)
		if err != nil {
			if errors.Is(err, errPartitionOwnerNotAssigned) {
				h.logger.Debug(func() string { return fmt.Sprintf("invoking non-smart since: %s", err.Error()) })
				return h.invokeNonSmart(inv)
			}
			return int64(0), err
		}
		return groupID, nil
	}
	return h.invokeNonSmart(inv)
}

func (h *ConnectionInvocationHandler) invokeSmart(inv invocation.Invocation) (int64, error) {
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

func (h *ConnectionInvocationHandler) invokeNonSmart(inv invocation.Invocation) (int64, error) {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok && boundInvocation.Connection() != nil {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	}
	return h.sendToRandomAddress(inv)
}

func (h *ConnectionInvocationHandler) sendToConnection(inv invocation.Invocation, conn *Connection) (int64, error) {
	if sent := conn.send(inv); !sent {
		return 0, ihzerrors.NewIOError("packet not sent", nil)
	}
	return conn.connectionID, nil
}

func (h *ConnectionInvocationHandler) sendToAddress(inv invocation.Invocation, addr pubcluster.Address) (int64, error) {
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
			return 0, fmt.Errorf("address not found: %s", addr.String())
		}
	}
	return h.sendToConnection(inv, conn)
}

func (h *ConnectionInvocationHandler) sendToRandomAddress(inv invocation.Invocation) (int64, error) {
	if conn := h.connectionManager.RandomConnection(); conn == nil {
		// TODO: use correct error type
		return 0, ihzerrors.NewIOError("no connection found", nil)
	} else {
		return h.sendToConnection(inv, conn)
	}
}
