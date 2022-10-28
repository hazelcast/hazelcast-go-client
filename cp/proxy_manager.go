package cp

import (
	"context"
	"fmt"
	"sync"
)

type ProxyManager struct {
	bundle  CpCreationBundle
	mu      *sync.RWMutex
	proxies map[string]interface{}
}

func newCpProxyManager(bundle CpCreationBundle) (*ProxyManager, error) {
	p := &ProxyManager{
		mu:      &sync.RWMutex{},
		proxies: map[string]interface{}{},
		bundle:  bundle,
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

	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	wrapper, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return wrapper, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if wrapper, ok := m.proxies[name]; ok {
		// someone has already created the proxy
		return wrapper, nil
	}

	p, err := newProxy(ctx, m.bundle, serviceName, objectName)
	if err != nil {
		return nil, err
	}
	wrapper, err = wrapProxyFn(p)
	if err != nil {
		return nil, err
	}
	m.proxies[name] = wrapper
	return wrapper, nil
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}

func (pm *ProxyManager) getAtomicLong(name string) (*AtomicLong, error) {
	p, err := pm.proxyFor(context.Background(), ATOMIC_LONG_SERVICE, name, func(p *proxy) (interface{}, error) {
		return newAtomicLong(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*AtomicLong), nil
}

func (*ProxyManager) getGroupId(proxyName string) (string, error) {
	return "", nil
}
