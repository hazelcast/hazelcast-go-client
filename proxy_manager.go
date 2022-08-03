/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hazelcast

import (
	"context"
	"fmt"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type proxyManager struct {
	mu              *sync.RWMutex
	proxies         map[string]interface{}
	invocationProxy *proxy
	serviceBundle   creationBundle
	refIDGenerator  *iproxy.ReferenceIDGenerator
	ncmDestroyFn    func(service, object string)
}

func newProxyManager(bundle creationBundle) *proxyManager {
	bundle.Check()
	pm := &proxyManager{
		mu:             &sync.RWMutex{},
		proxies:        map[string]interface{}{},
		serviceBundle:  bundle,
		refIDGenerator: iproxy.NewReferenceIDGenerator(1),
		ncmDestroyFn:   bundle.NCMDestroyFn,
	}
	p, err := newProxy(context.Background(), pm.serviceBundle, "", "", pm.refIDGenerator, func(ctx context.Context) bool { return false }, false)
	if err != nil {
		// It actually never panics since the proxy is local.
		panic(err)
	}
	pm.invocationProxy = p
	return pm
}

func (m *proxyManager) Proxies() map[string]interface{} {
	cp := make(map[string]interface{}, len(m.proxies))
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, p := range m.proxies {
		cp[k] = p
	}
	return cp
}

func (m *proxyManager) getMap(ctx context.Context, name string) (*Map, error) {
	p, err := m.proxyFor(ctx, ServiceNameMap, name, func(p *proxy) (interface{}, error) {
		return newMap(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*Map), nil
}

func (m *proxyManager) getReplicatedMap(ctx context.Context, name string) (*ReplicatedMap, error) {
	p, err := m.proxyFor(ctx, ServiceNameReplicatedMap, name, func(p *proxy) (interface{}, error) {
		return newReplicatedMap(p, m.refIDGenerator)
	})
	if err != nil {
		return nil, err
	}
	return p.(*ReplicatedMap), nil
}

func (m *proxyManager) getMultiMap(ctx context.Context, name string) (*MultiMap, error) {
	p, err := m.proxyFor(ctx, ServiceNameMultiMap, name, func(p *proxy) (interface{}, error) {
		return newMultiMap(p), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*MultiMap), nil
}

func (m *proxyManager) getQueue(ctx context.Context, name string) (*Queue, error) {
	p, err := m.proxyFor(ctx, ServiceNameQueue, name, func(p *proxy) (interface{}, error) {
		return newQueue(p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*Queue), nil
}

func (m *proxyManager) getTopic(ctx context.Context, name string) (*Topic, error) {
	p, err := m.proxyFor(ctx, ServiceNameTopic, name, func(p *proxy) (interface{}, error) {
		return newTopic(p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*Topic), nil
}

func (m *proxyManager) getList(ctx context.Context, name string) (*List, error) {
	p, err := m.proxyFor(ctx, ServiceNameList, name, func(p *proxy) (interface{}, error) {
		return newList(p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*List), nil
}

func (m *proxyManager) getSet(ctx context.Context, name string) (*Set, error) {
	p, err := m.proxyFor(ctx, ServiceNameSet, name, func(p *proxy) (interface{}, error) {
		return newSet(p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*Set), nil
}

func (m *proxyManager) getPNCounter(ctx context.Context, name string) (*PNCounter, error) {
	p, err := m.proxyFor(ctx, ServiceNamePNCounter, name, func(p *proxy) (interface{}, error) {
		return newPNCounter(ctx, p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*PNCounter), nil
}

func (m *proxyManager) getFlakeIDGenerator(ctx context.Context, name string) (*FlakeIDGenerator, error) {
	p, err := m.proxyFor(ctx, ServiceNamePNCounter, name, func(p *proxy) (interface{}, error) {
		return newFlakeIdGenerator(p, m.getFlakeIDGeneratorConfig(name), flakeIDBatchFromMemberFn), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*FlakeIDGenerator), nil
}

func (m *proxyManager) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return m.invocationProxy.invokeOnRandomTarget(ctx, request, handler)
}

func (m *proxyManager) addDistributedObjectEventListener(ctx context.Context, handler DistributedObjectNotifiedHandler) (types.UUID, error) {
	request := codec.EncodeClientAddDistributedObjectListenerRequest(!m.serviceBundle.Config.Cluster.Unisocket)
	subscriptionID := types.NewUUID()
	removeRequest := codec.EncodeClientRemoveDistributedObjectListenerRequest(subscriptionID)
	listenerHandler := func(msg *proto.ClientMessage) {
		codec.HandleClientAddDistributedObjectListener(msg, func(name string, service string, eventType string, source types.UUID) {
			handler(newDistributedObjectNotified(service, name, DistributedObjectEventType(eventType)))
		})
	}
	if err := m.serviceBundle.ListenerBinder.Add(ctx, subscriptionID, request, removeRequest, listenerHandler); err != nil {
		return types.UUID{}, err
	}
	return subscriptionID, nil
}

func (m *proxyManager) removeDistributedObjectEventListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.serviceBundle.ListenerBinder.Remove(ctx, subscriptionID)
}

func (m *proxyManager) remove(ctx context.Context, serviceName string, objectName string) bool {
	// assumes that m.mu mutex is already locked by the caller side
	name := makeProxyName(serviceName, objectName)
	p, ok := m.proxies[name]
	if !ok {
		return false
	}
	// run the local destroy method of Map
	if serviceName == ServiceNameMap {
		mp := p.(*Map)
		mp.destroyLocally(ctx)
		m.ncmDestroyFn(serviceName, objectName)
	}
	delete(m.proxies, name)
	return true
}

func (m *proxyManager) proxyFor(
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
	p, err := newProxy(ctx, m.serviceBundle, serviceName, objectName, m.refIDGenerator, func(ctx context.Context) bool {
		return m.remove(ctx, serviceName, objectName)
	}, true)
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

func (m *proxyManager) getFlakeIDGeneratorConfig(name string) FlakeIDGeneratorConfig {
	if conf, ok := m.serviceBundle.Config.FlakeIDGenerators[name]; ok {
		return conf
	}
	return FlakeIDGeneratorConfig{
		PrefetchCount:  defaultFlakeIDPrefetchCount,
		PrefetchExpiry: defaultFlakeIDPrefetchExpiry,
	}
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}

type proxyDestroyer interface {
	Destroy(ctx context.Context) error
}

func (m *proxyManager) destroyProxies(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, p := range m.proxies {
		ds := p.(proxyDestroyer)
		if err := ds.Destroy(ctx); err != nil {
			m.serviceBundle.Logger.Errorf("proxy %s key cannot be destroyed: %w", key, err)
		}
	}
}
