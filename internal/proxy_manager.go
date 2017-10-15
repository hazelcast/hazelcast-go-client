package internal

import (
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"sync/atomic"
)

type ProxyManager struct {
	ReferenceId int64
	client      *HazelcastClient
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

func (proxyManager *ProxyManager) GetOrCreateProxy(serviceName *string, name *string) core.IDistributedObject {
	var ns string = *serviceName + *name
	if _, ok := proxyManager.proxies[ns]; ok {
		return proxyManager.proxies[ns]
	}
	proxy := proxyManager.createProxy(serviceName, name)
	proxyManager.proxies[ns] = proxy
	return proxy
}

func (proxyManager *ProxyManager) createProxy(serviceName *string, name *string) core.IDistributedObject {
	message := ClientCreateProxyEncodeRequest(name, serviceName, proxyManager.findNextProxyAddress())
	proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
	return proxyManager.getProxyByNameSpace(serviceName, name)
}

func (proxyManager *ProxyManager) destroyProxy(serviceName *string, name *string) bool {
	var ns string = *serviceName + *name
	if _, ok := proxyManager.proxies[ns]; ok {
		delete(proxyManager.proxies, ns)
		message := ClientDestroyProxyEncodeRequest(name, serviceName)
		proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
		return true
	}
	return false
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
