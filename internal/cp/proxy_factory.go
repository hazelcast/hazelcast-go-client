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

package cp

import (
	"context"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	atomicLongService   = "hz:raft:atomicLongService"
	atomicRefService    = "hz:raft:atomicRefService"
	cpMapService        = "hz:raft:mapService"
	defaultGroupName    = "default"
	metadataCPGroupName = "metadata"
)

type proxyFactory struct {
	is         *invocation.Service
	ss         *iserialization.Service
	invFactory *cluster.ConnectionInvocationFactory
	lg         *logger.LogAdaptor
}

func newProxyFactory(ss *iserialization.Service, invFactory *cluster.ConnectionInvocationFactory, is *invocation.Service, lg *logger.LogAdaptor) *proxyFactory {
	return &proxyFactory{
		is:         is,
		invFactory: invFactory,
		ss:         ss,
		lg:         lg,
	}
}

func (m *proxyFactory) getOrCreateProxy(ctx context.Context, service string, nameWithGroup string) (interface{}, error) {
	name, err := withoutDefaultGroupName(nameWithGroup)
	if err != nil {
		return nil, err
	}
	obj, err := objectNameForProxy(name)
	if err != nil {
		return nil, err
	}
	p := newProxy(m.ss, m.invFactory, m.is, m.lg, service, obj)
	gid, err := m.createGroupID(ctx, p, name)
	if err != nil {
		return nil, err
	}
	p.groupID = gid
	switch service {
	case atomicLongService:
		return &AtomicLong{p}, nil
	case atomicRefService:
		return &AtomicRef{p}, nil
	case cpMapService:
		return &Map{p}, nil
	}
	return nil, hzerrors.NewIllegalArgumentError("requested data structure is not supported by Go Client CP Subsystem", nil)
}

func (m *proxyFactory) createGroupID(ctx context.Context, p *proxy, proxyName string) (types.RaftGroupID, error) {
	request := codec.EncodeCPGroupCreateCPGroupRequest(proxyName)
	response, err := p.invokeOnRandomTarget(ctx, request, nil)
	if err != nil {
		return types.RaftGroupID{}, err
	}
	return codec.DecodeCPGroupCreateCPGroupResponse(response), nil
}

func objectNameForProxy(name string) (string, error) {
	// ported from: com.hazelcast.cp.internal.RaftService#getObjectNameForProxy
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name, nil
	}
	group := strings.TrimSpace(name[idx+1:])
	if group == "" {
		return "", hzerrors.NewIllegalArgumentError("custom CP group name cannot be empty string", nil)
	}
	obj := strings.TrimSpace(name[:idx])
	if obj == "" {
		return "", hzerrors.NewIllegalArgumentError("object name cannot be empty string", nil)
	}
	return obj, nil
}

func withoutDefaultGroupName(proxyName string) (string, error) {
	// ported from: com.hazelcast.cp.internal.RaftService#withoutDefaultGroupName
	name := strings.TrimSpace(proxyName)
	idx := strings.Index(name, "@")
	if idx == -1 {
		return name, nil
	}
	if ci := strings.Index(name[idx+1:], "@"); ci != -1 {
		return "", hzerrors.NewIllegalArgumentError("custom group name must be specified at most once", nil)
	}
	group := strings.TrimSpace(name[idx+1:])
	if n := strings.ToLower(group); n == metadataCPGroupName {
		return "", hzerrors.NewIllegalArgumentError("cp data structures cannot run on the METADATA CP group!", nil)
	}
	if strings.ToLower(group) == defaultGroupName {
		return name[:idx], nil
	}
	return name, nil
}

func (m *proxyFactory) getAtomicLong(ctx context.Context, name string) (*AtomicLong, error) {
	p, err := m.getOrCreateProxy(ctx, atomicLongService, name)
	if err != nil {
		return nil, err
	}
	return p.(*AtomicLong), nil
}

func (m *proxyFactory) getAtomicRef(ctx context.Context, name string) (*AtomicRef, error) {
	p, err := m.getOrCreateProxy(ctx, atomicRefService, name)
	if err != nil {
		return nil, err
	}
	return p.(*AtomicRef), nil
}

func (m *proxyFactory) getMap(ctx context.Context, name string) (*Map, error) {
	p, err := m.getOrCreateProxy(ctx, cpMapService, name)
	if err != nil {
		return nil, err
	}
	return p.(*Map), nil
}
