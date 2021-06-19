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

package cloud

import (
	"context"
	"fmt"
	"sync"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

type AddressTranslator struct {
	dc         *DiscoveryClient
	translator map[string]pubcluster.Address
	mu         *sync.RWMutex
}

func NewAddressTranslator(dc *DiscoveryClient, addrs []Address) *AddressTranslator {
	t := &AddressTranslator{
		dc:         dc,
		translator: map[string]pubcluster.Address{},
		mu:         &sync.RWMutex{},
	}
	t.updateTranslator(addrs)
	return t
}

func (a *AddressTranslator) Translate(ctx context.Context, address pubcluster.Address) (pubcluster.Address, error) {
	a.mu.RLock()
	pubAddr, ok := a.translator[address.String()]
	a.mu.RUnlock()
	if ok {
		return pubAddr, nil
	}
	// address not found, try discovering the nodes
	if err := a.reload(ctx); err != nil {
		return address, err
	}
	a.mu.RLock()
	pubAddr, ok = a.translator[address.String()]
	a.mu.RUnlock()
	if ok {
		return pubAddr, nil
	}
	// address still not found, fail
	return address, fmt.Errorf("address not found: %s", address.String())
}

func (a *AddressTranslator) TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (addr pubcluster.Address, err error) {
	return a.Translate(ctx, member.Address)
}

func (a *AddressTranslator) reload(ctx context.Context) error {
	addrs, err := a.dc.DiscoverNodes(ctx)
	if err != nil {
		return err
	}
	a.updateTranslator(addrs)
	return nil
}

func (a *AddressTranslator) updateTranslator(addrs []Address) {
	t := map[string]pubcluster.Address{}
	for _, addr := range addrs {
		t[addr.Private] = pubcluster.Address(addr.Public)
	}
	a.mu.Lock()
	a.translator = t
	a.mu.Unlock()
}
