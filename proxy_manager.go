package hazelcast

import (
	"fmt"
	"math/rand"
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

func (m *proxyManager) GetMap(name string) (*Map, error) {
	if p, err := m.proxyFor("hz:impl:mapService", name); err != nil {
		return nil, err
	} else {
		return NewMapImpl(p), nil
	}
}

func (m *proxyManager) GetReplicatedMap(objectName string) (*ReplicatedMapImpl, error) {
	// returns an interface to not depend on hztypes.ReplicatedMap
	// TODO: change return type to ReplicatedMap
	if p, err := m.proxyFor("hz:impl:replicatedMapService", objectName); err != nil {
		return nil, err
	} else {
		partitionCount := m.serviceBundle.PartitionService.PartitionCount()
		partitionID := rand.Int31n(partitionCount)
		return NewReplicatedMapImpl(p, partitionID), nil
	}
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

func (m *proxyManager) proxyFor(serviceName string, objectName string) (*proxy, error) {
	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	obj, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return obj, nil
	}
	if p, err := m.createProxy(serviceName, objectName); err != nil {
		return nil, err
	} else {
		m.mu.Lock()
		m.proxies[name] = p
		m.mu.Unlock()
		return p, nil
	}
}

func (m proxyManager) createProxy(serviceName string, objectName string) (*proxy, error) {
	return NewProxy(m.serviceBundle, serviceName, objectName), nil
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
