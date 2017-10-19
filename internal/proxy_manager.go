package internal

import (
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"sync"
	"sync/atomic"
)

type ProxyManager struct {
	ReferenceId int64
	client      *HazelcastClient
	mu          sync.RWMutex // guards proxies
	proxies     map[string]core.IDistributedObject
}

func newProxyManager(client *HazelcastClient) *ProxyManager {
	return &ProxyManager{
		ReferenceId: 0,
		client:      client,
		proxies:     make(map[string]core.IDistributedObject),
	}
}

func (proxyManager *ProxyManager) nextReferenceId() int64 {
	return atomic.AddInt64(&proxyManager.ReferenceId, 1)
}

func (proxyManager *ProxyManager) GetOrCreateProxy(serviceName *string, name *string) (core.IDistributedObject, error) {
	var ns string = *serviceName + *name
	proxyManager.mu.RLock()
	if _, ok := proxyManager.proxies[ns]; ok {
		defer proxyManager.mu.RUnlock()
		return proxyManager.proxies[ns], nil
	}
	proxyManager.mu.RUnlock()
	proxy, err := proxyManager.createProxy(serviceName, name)
	if err != nil {
		return nil, err
	}
	proxyManager.mu.Lock()
	proxyManager.proxies[ns] = proxy
	proxyManager.mu.Unlock()
	return proxy, nil
}

func (proxyManager *ProxyManager) createProxy(serviceName *string, name *string) (core.IDistributedObject, error) {
	message := ClientCreateProxyEncodeRequest(name, serviceName, proxyManager.findNextProxyAddress())
	_, err := proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
	if err != nil {
		return nil, err
	}
	return proxyManager.getProxyByNameSpace(serviceName, name), nil
}

func (proxyManager *ProxyManager) destroyProxy(serviceName *string, name *string) (bool, error) {
	var ns string = *serviceName + *name
	proxyManager.mu.RLock()
	if _, ok := proxyManager.proxies[ns]; ok {
		proxyManager.mu.RUnlock()
		proxyManager.mu.Lock()
		delete(proxyManager.proxies, ns)
		proxyManager.mu.Unlock()
		message := ClientDestroyProxyEncodeRequest(name, serviceName)
		_, err := proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (proxyManager *ProxyManager) findNextProxyAddress() *Address {
	return proxyManager.client.LoadBalancer.NextAddress()
}

func (proxyManager *ProxyManager) getProxyByNameSpace(serviceName *string, name *string) core.IDistributedObject {
	if common.SERVICE_NAME_MAP == *serviceName {
		return &MapProxy{&proxy{proxyManager.client, serviceName, name}}
	}
	return nil
}
