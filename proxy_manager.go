/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal/client"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type proxyManager struct {
	proxies        *sync.Map
	invoker        *client.Invoker
	serviceBundle  creationBundle
	refIDGenerator *iproxy.ReferenceIDGenerator
	ncmDestroyFn   func(service, object string)
}

func newProxyManager(bundle creationBundle) *proxyManager {
	bundle.Check()
	return &proxyManager{
		proxies:        &sync.Map{},
		serviceBundle:  bundle,
		refIDGenerator: iproxy.NewReferenceIDGenerator(1),
		ncmDestroyFn:   bundle.NCMDestroyFn,
		invoker:        bundle.Invoker,
	}
}

func (m *proxyManager) Proxies() map[string]interface{} {
	cp := map[string]interface{}{}
	m.proxies.Range(func(key, value interface{}) bool {
		cp[key.(string)] = value
		return true
	})
	return cp
}

func (m *proxyManager) getMap(ctx context.Context, name string, fn func(p *proxy) (interface{}, error)) (*Map, error) {
	p, err := m.proxyFor(ctx, ServiceNameMap, name, fn)
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

func (m *proxyManager) getRingbuffer(ctx context.Context, name string) (*Ringbuffer, error) {
	p, err := m.proxyFor(ctx, ServiceNameRingBuffer, name, func(p *proxy) (interface{}, error) {
		return newRingbuffer(p)
	})
	if err != nil {
		return nil, err
	}
	return p.(*Ringbuffer), nil
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
	p, err := m.proxyFor(ctx, ServiceNameFlakeIDGenerator, name, func(p *proxy) (interface{}, error) {
		return newFlakeIdGenerator(p, m.getFlakeIDGeneratorConfig(name), flakeIDBatchFromMemberFn), nil
	})
	if err != nil {
		return nil, err
	}
	return p.(*FlakeIDGenerator), nil
}

func (m *proxyManager) invokeOnRandomTarget(ctx context.Context, request *proto.ClientMessage, handler proto.ClientMessageHandler) (*proto.ClientMessage, error) {
	return m.invoker.InvokeOnRandomTarget(ctx, request, handler)
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
	name := makeProxyName(serviceName, objectName)
	p, ok := m.proxies.Load(name)
	if !ok {
		return false
	}
	// run the local destroy method of Map
	if serviceName == ServiceNameMap {
		mp := p.(*Map)
		mp.destroyLocally(ctx)
		m.ncmDestroyFn(serviceName, objectName)
	}
	m.proxies.Delete(name)
	return true
}

func (m *proxyManager) proxyFor(ctx context.Context, serviceName string, objectName string, wrapProxyFn func(p *proxy) (interface{}, error)) (interface{}, error) {
	name := makeProxyName(serviceName, objectName)
	wrapper, ok := m.proxies.Load(name)
	if ok {
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
	m.proxies.Store(name, wrapper)
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
	return serviceName + objectName
}

type proxyDestroyer interface {
	removeFromCache(ctx context.Context) bool
}

func (m *proxyManager) destroyProxies(ctx context.Context) {
	for _, p := range m.Proxies() {
		p.(proxyDestroyer).removeFromCache(ctx)
	}
}
