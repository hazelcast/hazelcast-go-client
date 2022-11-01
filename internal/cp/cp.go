package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type CPSubSystem struct {
	proxyManager *proxyManager
}

func NewCPSubsystem(ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) *CPSubSystem {
	proxyManager, _ := newCpProxyManager(ss, cif, is, l)
	cp := &CPSubSystem{
		proxyManager: proxyManager,
	}
	return cp
}

func (c *CPSubSystem) GetAtomicLong(ctx context.Context, name string) (*AtomicLong, error) {
	return c.proxyManager.getAtomicLong(ctx, name)
}
