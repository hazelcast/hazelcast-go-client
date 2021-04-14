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
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	} else if inv.PartitionID() != -1 {
		if conn := h.connectionManager.GetConnForPartition(inv.PartitionID()); conn == nil {
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
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
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

func (h *ConnectionInvocationHandler) sendToOwnerAddress(inv invocation.Invocation) error {
	if addr := h.connectionManager.OwnerConnectionAddr(); addr == nil {
		return hzerror.NewHazelcastIOError("cannot send to owner address: not found", nil)
	} else {
		return h.sendToAddress(inv, addr)
	}
}
