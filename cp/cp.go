package cp

import (
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	ATOMIC_LONG_SERVICE      = "hz:raft:atomicLongService"
	ATOMIC_REFERENCE_SERVICE = "hz:raft:atomicRefService"
	COUNT_DOWN_LATCH_SERVICE = "hz:raft:countDownLatchService"
	LOCK_SERVICE             = "hz:raft:lockService"
	SEMAPHORE_SERVICE        = "hz:raft:semaphoreService"
)

type CpCreationBundle struct {
	InvocationService    *invocation.Service
	SerializationService *iserialization.Service
	Logger               *logger.LogAdaptor
}

func (b CpCreationBundle) Check() {
	if b.InvocationService == nil {
		panic("InvocationService is nil")
	}
	if b.SerializationService == nil {
		panic("SerializationService is nil")
	}
	if b.Logger == nil {
		panic("SerializationService is nil")
	}
}

type CpSubSystem struct {
	proxyManager *ProxyManager
}

func NewCpSubsystem(bundle CpCreationBundle) *CpSubSystem {
	bundle.Check()
	proxyManager, _ := newCpProxyManager(bundle)
	cp := &CpSubSystem{
		proxyManager: proxyManager,
	}
	return cp
}

func (c *CpSubSystem) GetAtomicLong(name string) (*AtomicLong, error) {
	return c.proxyManager.getAtomicLong()
}
