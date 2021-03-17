package cluster

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"time"
)

type ConnectionBoundInvocation interface {
	invocation.Invocation
	Connection() *ConnectionImpl
	StoreSentConnection(conn interface{})
}

type invocationImpl = invocation.Impl

type ConnectionBoundInvocationImpl struct {
	*invocationImpl
	boundConnection *ConnectionImpl
}

func NewConnectionBoundInvocation(clientMessage *proto.ClientMessage, partitionID int32, address pubcluster.Address,
	connection *ConnectionImpl, timeout time.Duration) *ConnectionBoundInvocationImpl {
	return &ConnectionBoundInvocationImpl{
		invocationImpl:  invocation.NewImpl(clientMessage, partitionID, address, timeout),
		boundConnection: connection,
	}
}

func (i *ConnectionBoundInvocationImpl) Connection() *ConnectionImpl {
	return i.boundConnection
}
