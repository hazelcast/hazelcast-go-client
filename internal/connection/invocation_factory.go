package connection

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/partition"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"time"
)

type InvocationFactory struct {
	invocationTimeout time.Duration
	partitionService  partition.Service
}

func NewInvocationFactory(partitionService partition.Service, invocationTimeout time.Duration) *InvocationFactory {
	if partitionService == nil {
		panic("partitionService is nil")
	}
	return &InvocationFactory{
		invocationTimeout: invocationTimeout,
		partitionService:  partitionService,
	}
}

func (f InvocationFactory) NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) invocation.Invocation {
	return invocation.NewImpl(message, partitionID, nil, f.invocationTimeout)
}

func (f InvocationFactory) NewInvocationOnRandomTarget(message *proto.ClientMessage) invocation.Invocation {
	return invocation.NewImpl(message, -1, nil, f.invocationTimeout)
}

func (f InvocationFactory) NewInvocationOnKeyOwner(message *proto.ClientMessage, data serialization.Data) invocation.Invocation {
	partitionID := f.partitionService.GetPartitionID(data)
	return f.NewInvocationOnPartitionOwner(message, partitionID)
}

func (f InvocationFactory) NewInvocationOnTarget(message *proto.ClientMessage, address *core.Address) invocation.Invocation {
	return invocation.NewImpl(message, -1, address, f.invocationTimeout)
}
