package connection

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
)

type InvocationHandlerCreationBundle struct {
	ConnectionManager Manager
	ClusterService    cluster.Service
	SmartRouting      bool
	Logger            logger.Logger
}

func (b InvocationHandlerCreationBundle) Check() {
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

type InvocationHandler struct {
	connectionManager Manager
	clusterService    cluster.Service
	smart             bool
	logger            logger.Logger
}

func NewInvocationHandler(bundle InvocationHandlerCreationBundle) *InvocationHandler {
	bundle.Check()
	return &InvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		smart:             bundle.SmartRouting,
		logger:            bundle.Logger,
	}
}

func (h InvocationHandler) Invoke(invocation invocation.Invocation) error {
	if h.smart {
		return h.invokeSmart(invocation)
	} else {
		return h.invokeNonSmart(invocation)
	}
}

func (h InvocationHandler) invokeSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(BoundInvocation); ok {
		return h.sendToConnection(inv, boundInvocation.Connection())
	} else if inv.PartitionID() != -1 {
		// XXX: ???
		return h.sendToRandomAddress(inv)
	} else if inv.Address() != nil {
		return h.sendToAddress(inv, inv.Address())
	} else {
		return h.sendToRandomAddress(inv)
	}
}

func (h InvocationHandler) invokeNonSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(BoundInvocation); ok {
		return h.sendToConnection(inv, boundInvocation.Connection())
	} else if addr := h.clusterService.OwnerConnectionAddress(); addr == nil {
		return core.NewHazelcastIOError("no address found to invoke", nil)
	} else {
		return h.sendToAddress(inv, addr)
	}
}

func (h InvocationHandler) sendToConnection(inv invocation.Invocation, conn *Impl) error {
	if sent := conn.send(inv.Request()); !sent {
		return core.NewHazelcastIOError("packet is not sent", nil)
	}
	inv.StoreSentConnection(conn)
	return nil
}

func (n InvocationHandler) sendToAddress(inv invocation.Invocation, addr *core.Address) error {
	if conn, err := n.connectionManager.ConnectionForAddress(addr); err != nil {
		n.logger.Trace("Sending invocation to ", inv.Address(), " failed, err: ", err)
		return err
	} else {
		return n.sendToConnection(inv, conn)
	}
}

func (n InvocationHandler) sendToRandomAddress(inv invocation.Invocation) error {
	panic("implement me!")
}
