package hazelcast

import (
	"fmt"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/hztypes"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
)

const (
	ReplicatedMapService = "hz:impl:replicatedMapService"
)

type proxyManager struct {
	mu            *sync.RWMutex
	proxies       map[string]*proxy.Proxy
	serviceBundle proxy.CreationBundle
}

func newManager(bundle proxy.CreationBundle) *proxyManager {
	bundle.Check()
	return &proxyManager{
		mu:            &sync.RWMutex{},
		proxies:       map[string]*proxy.Proxy{},
		serviceBundle: bundle,
	}
}

func (m *proxyManager) GetMap(name string) (*hztypes.Map, error) {
	if p, err := m.proxyFor("hz:impl:mapService", name); err != nil {
		return nil, err
	} else {
		return hztypes.NewMapImpl(p), nil
	}
}

/*
func (m *proxyManager) GetReplicatedMap(objectName string) (interface{}, error) {
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

func (m *proxyManager) Remove(serviceName string, objectName string) error {
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

func (m *proxyManager) proxyFor(serviceName string, objectName string) (*proxy.Proxy, error) {
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

func (m proxyManager) createProxy(serviceName string, objectName string) (*proxy.Proxy, error) {
	return proxy.NewProxy(m.serviceBundle, serviceName, objectName), nil
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
