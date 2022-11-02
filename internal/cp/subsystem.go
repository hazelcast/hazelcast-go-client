package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/cp"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

var (
	_ cp.SubSystem = SubSystem{}
)

type SubSystem struct {
	proxyManager *proxyManager
}

func NewCPSubsystem(ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) SubSystem {
	var c SubSystem
	proxyManager, _ := newCpProxyManager(ss, cif, is, l)
	c.proxyManager = proxyManager
	return c
}

func (c SubSystem) GetAtomicLong(ctx context.Context, name string) (cp.AtomicLong, error) {
	res, err := c.proxyManager.getAtomicLong(ctx, name)
	return res, err
}
