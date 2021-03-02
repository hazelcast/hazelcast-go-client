package partition

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type CreationBundle struct {
	SerializationService serialization.Service
	Logger               logger.Logger
}

func (b CreationBundle) Check() {
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
}
