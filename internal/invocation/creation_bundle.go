package invocation

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/partition"
)

type CreationBundle struct {
	PartitionService partition.Service
	Handler          Handler
	SmartRouting     bool
	Logger           logger.Logger
}

func (b CreationBundle) Check() {
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.Handler == nil {
		panic("Handler is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}
