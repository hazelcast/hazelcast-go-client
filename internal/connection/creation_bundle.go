package connection

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
)

type CreationBundle struct {
	//InvocationService invocation.Service
	SmartRouting      bool
	Logger            logger.Logger
	AddressTranslator internal.AddressTranslator
}

func (b CreationBundle) Check() {
	//if b.InvocationService == nil {
	//	panic("InvocationService is nil")
	//}
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.AddressTranslator == nil {
		panic("AddressTranslator is nil")
	}
}
