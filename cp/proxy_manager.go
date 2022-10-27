package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

type ProxyManager struct {
	serializationService *iserialization.Service
	invocationService    *invocation.Service
	logger               *logger.LogAdaptor
	name                 string
}

func newCpProxyManager(bundle CpCreationBundle) (*ProxyManager, error) {
	p := &ProxyManager{
		name:                 "dsadas",
		invocationService:    bundle.InvocationService,
		serializationService: bundle.SerializationService,
		logger:               bundle.Logger,
	}
	return p, nil
}

func (*ProxyManager) GetOrCreateProxy() (*proxy, error) {
	return nil, nil
}

func (m *ProxyManager) proxyFor(
	ctx context.Context,
	serviceName string,
	objectName string,
	wrapProxyFn func(p *proxy) (interface{}, error)) (interface{}, error) {
	return nil, nil
}

func (pm *ProxyManager) getAtomicLong() (*AtomicLong, error) {
	p, err := pm.proxyFor(context.Background(), ATOMIC_LONG_SERVICE, "dsa", func(p *proxy) (interface{}, error) {
		return NewAtomicLong(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*AtomicLong), nil
}

func (*ProxyManager) getGroupId(proxyName string) (string, error) {
	return "", nil
}
