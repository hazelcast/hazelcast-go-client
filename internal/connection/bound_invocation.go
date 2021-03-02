package connection

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"time"
)

type BoundInvocation interface {
	invocation.Invocation
	Connection() *Impl
}

type invocationImpl = invocation.Impl

type BoundInvocationImpl struct {
	*invocationImpl
	boundConnection *Impl
}

func NewBoundInvocation(clientMessage *proto.ClientMessage, partitionID int32, address *core.Address,
	connection *Impl, timeout time.Duration) *BoundInvocationImpl {
	return &BoundInvocationImpl{
		invocationImpl:  invocation.NewImpl(clientMessage, partitionID, address, timeout),
		boundConnection: connection,
	}
}

func (i *BoundInvocationImpl) Connection() *Impl {
	return i.boundConnection
}

func (i *BoundInvocationImpl) isBoundToSingleConnection() bool {
	return i.boundConnection != nil
}
