package cluster

import (
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type ConnectionInvocationFactory struct {
	invocationTimeout time.Duration
	nextCorrelationID int64
}

func NewConnectionInvocationFactory(invocationTimeout time.Duration) *ConnectionInvocationFactory {
	return &ConnectionInvocationFactory{
		invocationTimeout: invocationTimeout,
	}
}

func (f *ConnectionInvocationFactory) NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) invocation.Invocation {
	message.SetCorrelationID(f.makeCorrelationID())
	return invocation.NewImpl(message, partitionID, nil, f.invocationTimeout)
}

func (f *ConnectionInvocationFactory) NewInvocationOnRandomTarget(message *proto.ClientMessage, handler proto.ClientMessageHandler) invocation.Invocation {
	message.SetCorrelationID(f.makeCorrelationID())
	inv := invocation.NewImpl(message, -1, nil, f.invocationTimeout)
	inv.SetEventHandler(handler)
	return inv
}

func (f *ConnectionInvocationFactory) NewInvocationOnTarget(message *proto.ClientMessage, address pubcluster.Address) invocation.Invocation {
	message.SetCorrelationID(f.makeCorrelationID())
	return invocation.NewImpl(message, -1, address, f.invocationTimeout)
}

func (f *ConnectionInvocationFactory) NewConnectionBoundInvocation(message *proto.ClientMessage, partitionID int32, address pubcluster.Address,
	connection *Connection, timeout time.Duration) *ConnectionBoundInvocation {
	message.SetCorrelationID(f.makeCorrelationID())
	return newConnectionBoundInvocation(message, partitionID, address, connection, timeout)
}

func (f *ConnectionInvocationFactory) makeCorrelationID() int64 {
	return atomic.AddInt64(&f.nextCorrelationID, 1)
}
