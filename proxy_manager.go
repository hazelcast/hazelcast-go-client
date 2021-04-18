package hazelcast

import (
	"fmt"
	"sync"
)

type proxyManager struct {
	mu            *sync.RWMutex
	proxies       map[string]*proxy
	serviceBundle CreationBundle
}

func newManager(bundle CreationBundle) *proxyManager {
	bundle.Check()
	return &proxyManager{
		mu:            &sync.RWMutex{},
		proxies:       map[string]*proxy{},
		serviceBundle: bundle,
	}
}

func (m *proxyManager) GetMap(name string) *Map {
	p := m.proxyFor("hz:impl:mapService", name)
	return NewMapImpl(p)
}

func (m *proxyManager) GetReplicatedMap(objectName string) *ReplicatedMap {
	p := m.proxyFor("hz:impl:replicatedMapService", objectName)
	return NewReplicatedMapImpl(p)
}

func (m *proxyManager) Remove(serviceName string, objectName string) error {
	name := makeProxyName(serviceName, objectName)
	m.mu.Lock()
	p, ok := m.proxies[name]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.proxies, name)
	m.mu.Unlock()
	return p.Destroy()
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

func (m proxyManager) createProxy(serviceName string, objectName string) *proxy {
	return NewProxy(m.serviceBundle, serviceName, objectName)
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
