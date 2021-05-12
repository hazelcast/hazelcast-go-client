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
	"fmt"
	"sync"

	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
)

const (
	lockIDKey = "__hz_lockid"
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
		refIDGenerator: iproxy.NewReferenceIDGenerator(),
	}
}

func (m *proxyManager) getMap(name string) (*Map, error) {
	if p, err := m.proxyFor("hz:impl:mapService", name); err != nil {
		return nil, err
	} else {
		return newMap(p), nil
	}
}

func (m *proxyManager) getReplicatedMap(objectName string) (*ReplicatedMap, error) {
	if p, err := m.proxyFor("hz:impl:replicatedMapService", objectName); err != nil {
		return nil, err
	} else {
		return newReplicatedMapImpl(p)
	}
}

func (m *proxyManager) getQueue(objectName string) (*Queue, error) {
	if p, err := m.proxyFor("hz:impl:queueService", objectName); err != nil {
		return nil, err
	} else {
		return newQueue(p)
	}
}

func (m *proxyManager) getTopic(objectName string) (*Topic, error) {
	if p, err := m.proxyFor("hz:impl:topicService", objectName); err != nil {
		return nil, err
	} else {
		return newTopic(p)
	}
}

func (m *proxyManager) getList(objectName string) (*List, error) {
	if p, err := m.proxyFor("hz:impl:listService", objectName); err != nil {
		return nil, err
	} else {
		return newList(p)
	}
}

func (m *proxyManager) remove(serviceName string, objectName string) error {
	name := makeProxyName(serviceName, objectName)
	m.mu.Lock()
	p, ok := m.proxies[name]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	delete(m.proxies, name)
	m.mu.Unlock()
	return p.Destroy()
}

func (m *proxyManager) proxyFor(serviceName string, objectName string) (*proxy, error) {
	name := makeProxyName(serviceName, objectName)
	m.mu.RLock()
	obj, ok := m.proxies[name]
	m.mu.RUnlock()
	if ok {
		return obj, nil
	}
	if p, err := m.createProxy(serviceName, objectName); err != nil {
		return nil, err
	} else {
		m.mu.Lock()
		m.proxies[name] = p
		m.mu.Unlock()
		return p, nil
	}
}

func (m *proxyManager) createProxy(serviceName string, objectName string) (*proxy, error) {
	return newProxy(m.serviceBundle, serviceName, objectName, m.refIDGenerator)
}

func makeProxyName(serviceName string, objectName string) string {
	return fmt.Sprintf("%s%s", serviceName, objectName)
}
