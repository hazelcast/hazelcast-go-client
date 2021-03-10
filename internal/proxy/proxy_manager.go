package proxy

import (
	"fmt"
	pubproxy "github.com/hazelcast/hazelcast-go-client/v4/proxy"
	"sync"
)

type Manager interface {
	GetMap(name string) (pubproxy.Map, error)
	Remove(serviceName string, objectName string) error
}

type ManagerImpl struct {
	mu            *sync.RWMutex
	proxies       map[string]*Impl
	serviceBundle CreationBundle
}

func NewManagerImpl(bundle CreationBundle) *ManagerImpl {
	bundle.Check()
	return &ManagerImpl{
		mu:            &sync.RWMutex{},
		proxies:       map[string]*Impl{},
		serviceBundle: bundle,
	}
}

func (m *ManagerImpl) GetMap(objectName string) (pubproxy.Map, error) {
	if proxy, err := m.proxyFor(MapServiceName, objectName); err != nil {
		return nil, err
	} else {
		return NewMapImpl(proxy), nil
	}
}

func (m *ManagerImpl) Remove(serviceName string, objectName string) error {
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

func (m *ManagerImpl) proxyFor(serviceName string, objectName string) (*Impl, error) {
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

func (m ManagerImpl) createProxy(serviceName string, objectName string) (*Impl, error) {
	return NewImpl(m.serviceBundle, serviceName, objectName), nil
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
