package hazelcast

import (
	"context"
	"fmt"
	"sync"

	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
)

const (
	lockIDKey = "__hz_lockid"
)

type proxyManager struct {
	mu             *sync.RWMutex
	proxies        map[string]*proxy
	serviceBundle  creationBundle
	refIDGenerator iproxy.ReferenceIDGenerator
}

func newManager(bundle creationBundle) *proxyManager {
	bundle.Check()
	return &proxyManager{
		mu:             &sync.RWMutex{},
		proxies:        map[string]*proxy{},
		serviceBundle:  bundle,
		refIDGenerator: iproxy.NewReferenceIDGeneratorImpl(),
	}
}

func (m *proxyManager) getMapWithContext(ctx context.Context, name string) *Map {
	p := m.proxyFor("hz:impl:mapService", name)
	ctx = context.WithValue(ctx, lockIDKey, m.refIDGenerator.NextID())
	return newMap(ctx, p)
}

func (m *proxyManager) getReplicatedMap(objectName string) *ReplicatedMap {
	p := m.proxyFor("hz:impl:replicatedMapService", objectName)
	return NewReplicatedMapImpl(p)
}

func (m *proxyManager) remove(serviceName string, objectName string) error {
	name := makeProxyName(serviceName, objectName)
	m.mu.Lock()
	p, ok := m.proxies[name]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.proxies, name)
	m.mu.Unlock()
	return p.destroy()
}

func (m *proxyManager) proxyFor(serviceName string, objectName string) *proxy {
	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	obj, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return obj
	}
	p := m.createProxy(serviceName, objectName)
	m.mu.Lock()
	m.proxies[name] = p
	m.mu.Unlock()
	return p
}

func (m *proxyManager) createProxy(serviceName string, objectName string) *proxy {
	return newProxy(m.serviceBundle, serviceName, objectName)
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
