package proxy

import (
	"fmt"
	"sync"
)

const (
	ReplicatedMapService = "hz:impl:replicatedMapService"
)

type Manager struct {
	mu            *sync.RWMutex
	proxies       map[string]*Proxy
	serviceBundle CreationBundle
}

func NewManager(bundle CreationBundle) *Manager {
	bundle.Check()
	return &Manager{
		mu:            &sync.RWMutex{},
		proxies:       map[string]*Proxy{},
		serviceBundle: bundle,
	}
}

/*
func (m *Manager) GetReplicatedMap(objectName string) (interface{}, error) {
	// returns an interface to not depend on hztypes.ReplicatedMap
	// TODO: change return type to ReplicatedMap
	if proxy, err := m.proxyFor(ReplicatedMapService, objectName); err != nil {
		return nil, err
	} else {
		partitionCount := m.serviceBundle.PartitionService.PartitionCount()
		partitionID := rand.Int31n(partitionCount)
		return NewReplicatedMapImpl(proxy, partitionID), nil
	}
}
*/

func (m *Manager) Remove(serviceName string, objectName string) error {
	name := makeProxyName(serviceName, objectName)
	m.mu.Lock()
	proxy, ok := m.proxies[name]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.proxies, name)
	m.mu.Unlock()
	return proxy.Destroy()
}

func (m *Manager) ProxyFor(serviceName string, objectName string) (*Proxy, error) {
	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	obj, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return obj, nil
	}
	if proxy, err := m.createProxy(serviceName, objectName); err != nil {
		return nil, err
	} else {
		m.mu.Lock()
		m.proxies[name] = proxy
		m.mu.Unlock()
		return proxy, nil
	}
}

func (m Manager) createProxy(serviceName string, objectName string) (*Proxy, error) {
	return NewProxy(m.serviceBundle, serviceName, objectName), nil
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
