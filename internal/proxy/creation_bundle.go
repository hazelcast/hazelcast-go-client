package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/partition"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
)

type CreationBundle struct {
	SerializationService spi.SerializationService
	PartitionService     partition.Service
	InvocationService    invocation.Service
	ClusterService       cluster.Service
	InvocationFactory    invocation.Factory
	SmartRouting         bool
}

func (b CreationBundle) Check() {
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.InvocationService == nil {
		panic("InvocationService is nil")
	}
	if b.ClusterService == nil {
		panic("ClusterService is nil")
	}
	if b.InvocationFactory == nil {
		panic("InvocationFactory is nil")
	}
}
