package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"time"
)

type ConnectionBoundInvocation interface {
	invocation.Invocation
	Connection() *ConnectionImpl
}

type invocationImpl = invocation.Impl

type BoundInvocationImpl struct {
	*invocationImpl
	boundConnection *ConnectionImpl
}

func NewConnectionBoundInvocation(clientMessage *proto.ClientMessage, partitionID int32, address *core.Address,
	connection *ConnectionImpl, timeout time.Duration) *BoundInvocationImpl {
	return &BoundInvocationImpl{
		invocationImpl:  invocation.NewImpl(clientMessage, partitionID, address, timeout),
		boundConnection: connection,
	}
}

func (i *BoundInvocationImpl) Connection() *ConnectionImpl {
	return i.boundConnection
}

func (i *BoundInvocationImpl) isBoundToSingleConnection() bool {
	return i.boundConnection != nil
}
