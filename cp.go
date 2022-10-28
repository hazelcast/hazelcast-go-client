package hazelcast

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/cp"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type CPSubsystem struct {
	proxyManager *cp.ProxyManager
}

func NewCpSubsystem(ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) *CPSubsystem {
	proxyManager, _ := cp.NewCpProxyManager(ss, cif, is, l)
	cp := &CPSubsystem{
		proxyManager: proxyManager,
	}
	return cp
}

func (c *CPSubsystem) GetAtomicLong(ctx context.Context, name string) (*cp.AtomicLong, error) {
	return c.proxyManager.GetAtomicLong(ctx, name)
}
