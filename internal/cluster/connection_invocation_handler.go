package cluster

import (
	"errors"
	"fmt"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
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
}

func NewConnectionInvocationHandler(bundle ConnectionInvocationHandlerCreationBundle) *ConnectionInvocationHandler {
	bundle.Check()
	return &ConnectionInvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		logger:            bundle.Logger,
	}
}

func (h *ConnectionInvocationHandler) Invoke(invocation invocation.Invocation) error {
	if h.clusterService.SmartRoutingEnabled() {
		return h.invokeSmart(invocation)
	} else {
		return h.invokeNonSmart(invocation)
	}
}

func (h *ConnectionInvocationHandler) invokeSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
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
		return h.sendToRandomAddress(inv)
	}
}

func (h *ConnectionInvocationHandler) invokeNonSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	} else if addr := h.clusterService.OwnerConnectionAddr(); addr == nil {
		return h.sendToRandomAddress(inv)
	} else {
		return h.sendToAddress(inv, addr)
	}
}

func (h *ConnectionInvocationHandler) sendToConnection(inv invocation.Invocation, conn *Connection) error {
	if sent := conn.send(inv); !sent {
		return hzerror.NewHazelcastIOError("packet is not sent", nil)
	}
	return nil
}

func (h *ConnectionInvocationHandler) sendToAddress(inv invocation.Invocation, addr pubcluster.Address) error {
	if conn := h.connectionManager.GetConnectionForAddress(addr); conn == nil {
		h.logger.Tracef("Sending invocation to %s failed, address not found", addr.String())
		return fmt.Errorf("address not found: %s", addr.String())
	} else if invImpl, ok := inv.(*invocation.Impl); ok {
		boundInv := &ConnectionBoundInvocation{
			invocationImpl:  invImpl,
			boundConnection: conn,
		}
		return h.sendToConnection(boundInv, conn)
	} else {
		return errors.New("only invocations of time *invocationImpl is supported")
	}
}

func (h *ConnectionInvocationHandler) sendToRandomAddress(inv invocation.Invocation) error {
	if addr := h.clusterService.OwnerConnectionAddr(); addr == nil {
		return hzerror.NewHazelcastIOError("no address found to invoke", nil)
	} else {
		return h.sendToAddress(inv, addr)
	}
}
