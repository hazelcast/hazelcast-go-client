package cluster

import (
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type invocationImpl = invocation.Impl

type ConnectionBoundInvocation struct {
	*invocationImpl
	boundConnection *Connection
}

func newConnectionBoundInvocation(clientMessage *proto.ClientMessage, partitionID int32, address *pubcluster.AddressImpl,
	connection *Connection, timeout time.Duration) *ConnectionBoundInvocation {
	return &ConnectionBoundInvocation{
		invocationImpl:  invocation.NewImpl(clientMessage, partitionID, address, timeout),
		boundConnection: connection,
	}
}

func (i *ConnectionBoundInvocation) Connection() *Connection {
	return i.boundConnection
}

func (i *ConnectionBoundInvocation) SetEventHandler(handler proto.ClientMessageHandler) {
	i.invocationImpl.SetEventHandler(handler)
}
