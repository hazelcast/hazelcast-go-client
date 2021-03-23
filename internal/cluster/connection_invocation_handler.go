package cluster

import (
	"errors"
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	ihzerror "github.com/hazelcast/hazelcast-go-client/v4/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
)

type ConnectionInvocationHandlerCreationBundle struct {
	ConnectionManager *ConnectionManager
	ClusterService    Service
	SmartRouting      bool
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
	clusterService    Service
	smart             bool
	logger            logger.Logger
}

func NewConnectionInvocationHandler(bundle ConnectionInvocationHandlerCreationBundle) *ConnectionInvocationHandler {
	bundle.Check()
	return &ConnectionInvocationHandler{
		connectionManager: bundle.ConnectionManager,
		clusterService:    bundle.ClusterService,
		smart:             bundle.SmartRouting,
		logger:            bundle.Logger,
	}
}

func (h ConnectionInvocationHandler) Invoke(invocation invocation.Invocation) error {
	if h.smart {
		return h.invokeSmart(invocation)
	} else {
		return h.invokeNonSmart(invocation)
	}
}

func (h ConnectionInvocationHandler) invokeSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	} else if inv.PartitionID() != -1 {
		// XXX: ???
		return h.sendToRandomAddress(inv)
	} else if inv.Address() != nil {
		return h.sendToAddress(inv, inv.Address())
	} else {
		return h.sendToRandomAddress(inv)
	}
}

func (h ConnectionInvocationHandler) invokeNonSmart(inv invocation.Invocation) error {
	if boundInvocation, ok := inv.(*ConnectionBoundInvocation); ok {
		return h.sendToConnection(boundInvocation, boundInvocation.Connection())
	} else if addr := h.clusterService.OwnerConnectionAddr(); addr == nil {
		return hzerror.NewHazelcastIOError("no address found to invoke", nil)
	} else {
		return h.sendToAddress(inv, addr)
	}
}

func (h ConnectionInvocationHandler) sendToConnection(inv *ConnectionBoundInvocation, conn *Connection) error {
	if sent := conn.send(inv); !sent {
		return hzerror.NewHazelcastIOError("packet is not sent", nil)
	}
	return nil
}

func (n ConnectionInvocationHandler) sendToAddress(inv invocation.Invocation, addr pubcluster.Address) error {
	if conn := n.connectionManager.GetConnectionForAddress(addr); conn == nil {
		n.logger.Trace("Sending invocation to ", inv.Address(), " failed, address not found")
		return ihzerror.ErrAddressNotFound
	} else if invImpl, ok := inv.(*invocation.Impl); ok {
		boundInv := &ConnectionBoundInvocation{
			invocationImpl:  invImpl,
			boundConnection: conn,
		}
		return n.sendToConnection(boundInv, conn)
	} else {
		return errors.New("only invocations of time *invocationImpl is supported")
	}
}

func (n ConnectionInvocationHandler) sendToRandomAddress(inv invocation.Invocation) error {
	panic("sendToRandomAddress: implement me!")
}
