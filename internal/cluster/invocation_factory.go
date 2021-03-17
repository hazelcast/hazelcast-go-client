package cluster

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"time"
)

type ConnectionInvocationFactory struct {
	invocationTimeout time.Duration
	partitionService  PartitionService
}

func NewConnectionInvocationFactory(partitionService PartitionService, invocationTimeout time.Duration) *ConnectionInvocationFactory {
	if partitionService == nil {
		panic("partitionService is nil")
	}
	return &ConnectionInvocationFactory{
		invocationTimeout: invocationTimeout,
		partitionService:  partitionService,
	}
}

func (f ConnectionInvocationFactory) NewInvocationOnPartitionOwner(message *proto.ClientMessage, partitionID int32) invocation.Invocation {
	return invocation.NewImpl(message, partitionID, nil, f.invocationTimeout)
}

func (f ConnectionInvocationFactory) NewInvocationOnRandomTarget(message *proto.ClientMessage) invocation.Invocation {
	return invocation.NewImpl(message, -1, nil, f.invocationTimeout)
}

func (f ConnectionInvocationFactory) NewInvocationOnKeyOwner(message *proto.ClientMessage, data serialization.Data) invocation.Invocation {
	partitionID := f.partitionService.GetPartitionID(data)
	return f.NewInvocationOnPartitionOwner(message, partitionID)
}

func (f ConnectionInvocationFactory) NewInvocationOnTarget(message *proto.ClientMessage, address pubcluster.Address) invocation.Invocation {
	return invocation.NewImpl(message, -1, address, f.invocationTimeout)
}
