// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
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

func (proxyManager *ProxyManager) GetOrCreateProxy(serviceName string, name string) (core.IDistributedObject, error) {
	var ns string = serviceName + name
	proxyManager.mu.RLock()
	if _, ok := proxyManager.proxies[ns]; ok {
		defer proxyManager.mu.RUnlock()
		return proxyManager.proxies[ns], nil
	}
	proxyManager.mu.RUnlock()
	proxy, err := proxyManager.createProxy(&serviceName, &name)
	if err != nil {
		return nil, err
	}
	proxyManager.mu.Lock()
	proxyManager.proxies[ns] = proxy
	proxyManager.mu.Unlock()
	return proxy, nil
}

func (proxyManager *ProxyManager) createProxy(serviceName *string, name *string) (core.IDistributedObject, error) {
	message := ClientCreateProxyCodec.EncodeRequest(name, serviceName, proxyManager.findNextProxyAddress())
	_, err := proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
	if err != nil {
		return nil, err
	}
	return proxyManager.getProxyByNameSpace(serviceName, name)
}

func (proxyManager *ProxyManager) destroyProxy(serviceName *string, name *string) (bool, error) {
	var ns string = *serviceName + *name
	proxyManager.mu.RLock()
	if _, ok := proxyManager.proxies[ns]; ok {
		proxyManager.mu.RUnlock()
		proxyManager.mu.Lock()
		delete(proxyManager.proxies, ns)
		proxyManager.mu.Unlock()
		message := ClientDestroyProxyCodec.EncodeRequest(name, serviceName)
		_, err := proxyManager.client.InvocationService.InvokeOnRandomTarget(message).Result()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	proxyManager.mu.RUnlock()
	return false, nil
}

func (proxyManager *ProxyManager) findNextProxyAddress() *Address {
	return proxyManager.client.LoadBalancer.NextAddress()
}

func (proxyManager *ProxyManager) getProxyByNameSpace(serviceName *string, name *string) (core.IDistributedObject, error) {
	if common.SERVICE_NAME_MAP == *serviceName {
		return newMapProxy(proxyManager.client, serviceName, name), nil
	} else if common.SERVICE_NAME_LIST == *serviceName {
		return newListProxy(proxyManager.client, serviceName, name)
	}
	return nil, nil
}
