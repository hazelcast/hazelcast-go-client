/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
)

type proxyManager struct {
	mu             *sync.RWMutex
	proxies        map[string]*proxy
	serviceBundle  creationBundle
	refIDGenerator *iproxy.ReferenceIDGenerator
}

func newProxyManager(bundle creationBundle) *proxyManager {
	bundle.Check()
	return &proxyManager{
		mu:             &sync.RWMutex{},
		proxies:        map[string]*proxy{},
		serviceBundle:  bundle,
		refIDGenerator: iproxy.NewReferenceIDGenerator(1),
	}
}

func (m *proxyManager) getMap(ctx context.Context, name string) (*Map, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:mapService", name); err != nil {
		return nil, err
	} else {
		return newMap(p), nil
	}
}

func (m *proxyManager) getReplicatedMap(ctx context.Context, objectName string) (*ReplicatedMap, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:replicatedMapService", objectName); err != nil {
		return nil, err
	} else {
		return newReplicatedMap(p, m.refIDGenerator)
	}
}

func (m *proxyManager) getQueue(ctx context.Context, objectName string) (*Queue, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:queueService", objectName); err != nil {
		return nil, err
	} else {
		return newQueue(p)
	}
}

func (m *proxyManager) getTopic(ctx context.Context, objectName string) (*Topic, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:topicService", objectName); err != nil {
		return nil, err
	} else {
		return newTopic(p)
	}
}

func (m *proxyManager) getList(ctx context.Context, objectName string) (*List, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:listService", objectName); err != nil {
		return nil, err
	} else {
		return newList(p)
	}
}

func (m *proxyManager) getSet(ctx context.Context, objectName string) (*Set, error) {
	if p, err := m.proxyFor(ctx, "hz:impl:setService", objectName); err != nil {
		return nil, err
	} else {
		return newSet(p)
	}
}

func (m *proxyManager) remove(serviceName string, objectName string) bool {
	name := makeProxyName(serviceName, objectName)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.proxies[name]; !ok {
		return false
	}
	delete(m.proxies, name)
	return true
}

func (m *proxyManager) proxyFor(ctx context.Context, serviceName string, objectName string) (*proxy, error) {
	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	obj, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return obj, nil
	}
	if p, err := m.createProxy(ctx, serviceName, objectName); err != nil {
		return nil, err
	} else {
		m.mu.Lock()
		m.proxies[name] = p
		m.mu.Unlock()
		return p, nil
	}
}

func (m *proxyManager) createProxy(ctx context.Context, serviceName string, objectName string) (*proxy, error) {
	return newProxy(ctx, m.serviceBundle, serviceName, objectName, m.refIDGenerator, func() bool {
		return m.remove(serviceName, objectName)
	})
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
