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

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/cluster/discovery"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const (
	discoveryStrategyAdapterStarted = 1
)

type DiscoveryStrategyAdapter struct {
	strategy    discovery.Strategy
	lg          ilogger.LogAdaptor
	translator  map[pubcluster.Address]pubcluster.Address
	mu          *sync.RWMutex
	opts        discovery.StrategyOptions
	started     int32
	usePublicIP bool
}

func NewDiscoveryStrategyAdapter(cfg pubcluster.DiscoveryConfig, lg ilogger.LogAdaptor) *DiscoveryStrategyAdapter {
	if cfg.Strategy == nil {
		panic("discovery strategy is required")
	}
	return &DiscoveryStrategyAdapter{
		strategy:    cfg.Strategy,
		translator:  map[pubcluster.Address]pubcluster.Address{},
		usePublicIP: cfg.UsePublicIP,
		mu:          &sync.RWMutex{},
		lg:          lg,
		opts: discovery.StrategyOptions{
			Logger:      lg,
			UsePublicIP: cfg.UsePublicIP,
		},
	}
}

func (d *DiscoveryStrategyAdapter) Addresses(ctx context.Context) ([]pubcluster.Address, error) {
	d.lg.TraceHere()
	if err := d.ensureStarted(ctx); err != nil {
		return nil, err
	}
	nodes, err := d.strategy.DiscoverNodes(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]pubcluster.Address, 0, len(nodes))
	for _, node := range nodes {
		if node.PrivateAddr == "" && node.PublicAddr == "" {
			continue
		}
		if node.PrivateAddr == "" {
			res = append(res, (pubcluster.Address)(node.PublicAddr))
			continue
		}
		if node.PublicAddr == "" {
			res = append(res, (pubcluster.Address)(node.PrivateAddr))
			continue
		}
		if d.usePublicIP {
			res = append(res, (pubcluster.Address)(node.PublicAddr))
			continue
		}
		res = append(res, (pubcluster.Address)(node.PrivateAddr))
	}
	return res, nil
}

func (d *DiscoveryStrategyAdapter) TranslateMember(ctx context.Context, member *pubcluster.MemberInfo) (addr pubcluster.Address, err error) {
	d.lg.TraceHere()
	d.lg.Debug(func() string {
		return fmt.Sprintf("cluster.DiscoveryStrategyAdapter.TranslateMember: %s %s", member.UUID.String(), member.Address)
	})
	if err = d.ensureStarted(ctx); err != nil {
		return
	}
	addr, ok := d.translate(member.Address)
	if !ok {
		nodes, err := d.strategy.DiscoverNodes(ctx)
		if err != nil {
			return addr, err
		}
		d.updateTranslation(nodes)
		addr, ok = d.translate(member.Address)
	}
	if !ok {
		return addr, hzerrors.ErrAddressNotFound
	}
	return addr, nil
}

func (d *DiscoveryStrategyAdapter) ensureStarted(ctx context.Context) error {
	if atomic.LoadInt32(&d.started) != discoveryStrategyAdapterStarted {
		// discovery strategy was not started yet, try to start it
		if err := d.start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d *DiscoveryStrategyAdapter) start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if atomic.LoadInt32(&d.started) == discoveryStrategyAdapterStarted {
		// strategy was already started
		return nil
	}
	// run strategy's Start method if it implements that.
	if starter, ok := d.strategy.(discovery.StrategyStarter); ok {
		d.lg.Debug(func() string {
			return "Starting discovery strategy"
		})
		if err := starter.Start(ctx, d.opts); err != nil {
			return err
		}
	}
	atomic.StoreInt32(&d.started, discoveryStrategyAdapterStarted)
	return nil
}

func (d *DiscoveryStrategyAdapter) translate(addr pubcluster.Address) (pubcluster.Address, bool) {
	d.mu.RLock()
	addr, ok := d.translator[addr]
	d.mu.RUnlock()
	return addr, ok
}

func (d *DiscoveryStrategyAdapter) updateTranslation(nodes []discovery.Node) {
	translator := make(map[pubcluster.Address]pubcluster.Address, len(nodes))
	for _, node := range nodes {
		// map private addresses to public, if usePublicIP
		// map public IPs to public
		if node.PrivateAddr == "" && node.PublicAddr == "" {
			continue
		}
		if node.PublicAddr == "" {
			translator[pubcluster.Address(node.PrivateAddr)] = pubcluster.Address(node.PrivateAddr)
			continue
		}
		translator[pubcluster.Address(node.PublicAddr)] = pubcluster.Address(node.PublicAddr)
		if node.PrivateAddr == "" {
			continue
		}
		if d.usePublicIP {
			translator[pubcluster.Address(node.PrivateAddr)] = pubcluster.Address(node.PublicAddr)
		} else {
			translator[pubcluster.Address(node.PrivateAddr)] = pubcluster.Address(node.PrivateAddr)
		}
	}
	d.mu.Lock()
	d.translator = translator
	d.mu.Unlock()
}
