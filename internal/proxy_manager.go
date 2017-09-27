package internal

import (
	"sync/atomic"
	."github.com/hazelcast/go-client/internal/protocol"
)


type ProxyManager struct {
	ReferenceId int64
	client      *HazelcastClient
	proxies     map[*string]*proxy
}

const(
	MAP_SERVICE = "hz:impl:mapService"
	LIST_SERVICE = "hz:impl:listService"
)

func newProxyManager(client *HazelcastClient) *ProxyManager {
	return &ProxyManager{
		ReferenceId: 0,
		client:      client,
		proxies:     make(map[*string]*proxy),
	}
}

func (proxyManager *ProxyManager) nextReferenceId() int64 {
	return atomic.AddInt64(&proxyManager.ReferenceId, 1)
}

func (proxyManager *ProxyManager) GetOrCreateProxy(name *string, serviceName *string) *proxy{
	if proxyManager.proxies[name]!=nil {
		return proxyManager.proxies[name]
	} else {
		//var newProxy proxy = proxyManager.service[serviceName].Constructor(proxyManager.client, name).(IDistributedObject)
		var newProxy *proxy= &proxy{proxyManager.client, serviceName, name}


		proxyManager.createProxy(name, serviceName)
		proxyManager.proxies[name] = newProxy
		return newProxy
	}
}

func (proxyManager *ProxyManager) createProxy(name *string, serviceName *string) {
	connection:=proxyManager.client.ClusterService.getOwnerConnection()
	request := ClientCreateProxyEncodeRequest(name, serviceName, proxyManager.client.ClusterService.ownerConnectionAddress)
	proxyManager.client.InvocationService.InvokeOnConnection(request,connection)
}
