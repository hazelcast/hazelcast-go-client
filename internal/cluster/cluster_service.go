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

package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	ilogger "github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Service struct {
	logger            ilogger.Logger
	config            *pubcluster.Config
	eventDispatcher   *event.DispatchService
	partitionService  *PartitionService
	failoverService   *FailoverService
	invocationService *invocation.Service
	invocationFactory *ConnectionInvocationFactory
	membersMap        membersMap
}

type CreationBundle struct {
	Logger            ilogger.Logger
	InvocationFactory *ConnectionInvocationFactory
	EventDispatcher   *event.DispatchService
	PartitionService  *PartitionService
	FailoverService   *FailoverService
	Config            *pubcluster.Config
}

func (b CreationBundle) Check() {
	if b.InvocationFactory == nil {
		panic("InvocationFactory is nil")
	}
	if b.EventDispatcher == nil {
		panic("InvocationFactory is nil")
	}
	if b.PartitionService == nil {
		panic("PartitionService is nil")
	}
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.Config == nil {
		panic("Config is nil")
	}
}

func NewService(bundle CreationBundle) *Service {
	bundle.Check()
	return &Service{
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		partitionService:  bundle.PartitionService,
		failoverService:   bundle.FailoverService,
		logger:            bundle.Logger,
		config:            bundle.Config,
		membersMap:        newMembersMap(bundle.FailoverService, bundle.Logger),
	}
}

// SetInvocationService sets the invocation service for the cluster service.
func (s *Service) SetInvocationService(invService *invocation.Service) {
	s.invocationService = invService
}

func (s *Service) GetMemberByUUID(uuid types.UUID) *pubcluster.MemberInfo {
	return s.membersMap.Find(uuid)
}

func (s *Service) OrderedMembers() []pubcluster.MemberInfo {
	return s.membersMap.OrderedMembers()
}

func (s *Service) RefreshedSeedAddrs(clusterCtx *CandidateCluster) ([]pubcluster.Address, error) {
	s.membersMap.reset()
	addrSet := NewAddrSet()
	addrs, err := clusterCtx.AddressProvider.Addresses()
	if err != nil {
		return nil, err
	}
	addrSet.AddAddrs(addrs)
	return addrSet.Addrs(), nil
}

func (s *Service) TranslateMember(ctx context.Context, m *pubcluster.MemberInfo) (pubcluster.Address, error) {
	return s.failoverService.Current().AddressTranslator.TranslateMember(ctx, m)
}

func (s *Service) Reset() {
	s.membersMap.reset()
}

func (s *Service) handleMembersUpdated(conn *Connection, version int32, memberInfos []pubcluster.MemberInfo) {
	s.logger.Debug(func() string {
		return fmt.Sprintf("%d: members updated: %v", conn.connectionID, memberInfos)
	})
	added, removed := s.membersMap.Update(memberInfos, version)
	if len(added) > 0 {
		s.eventDispatcher.Publish(NewMembersAdded(added))
	}
	if len(removed) > 0 {
		s.eventDispatcher.Publish(NewMemberRemoved(removed))
	}
}

func (s *Service) sendMemberListViewRequest(ctx context.Context, conn *Connection) error {
	s.logger.Trace(func() string {
		return fmt.Sprintf("%d: cluster.Service.sendMemberListViewRequest", conn.connectionID)
	})
	request := codec.EncodeClientAddClusterViewListenerRequest()
	now := time.Now()
	inv := s.invocationFactory.NewConnectionBoundInvocation(request, conn, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.handleMembersUpdated(conn, version, memberInfos)
		}, func(version int32, partitions []proto.Pair) {
			s.partitionService.Update(conn.connectionID, partitions, version)
		})
	}, now)
	if err := s.invocationService.SendUrgentRequest(ctx, inv); err != nil {
		return err
	}
	_, err := inv.GetWithContext(ctx)
	return err
}

type AddrSet struct {
	addrs map[string]pubcluster.Address
}

func NewAddrSet() AddrSet {
	return AddrSet{addrs: map[string]pubcluster.Address{}}
}

func (a AddrSet) AddAddr(addr pubcluster.Address) {
	a.addrs[addr.String()] = addr
}

func (a AddrSet) AddAddrs(addrs []pubcluster.Address) {
	for _, addr := range addrs {
		a.AddAddr(addr)
	}
}

func (a AddrSet) Addrs() []pubcluster.Address {
	addrs := make([]pubcluster.Address, 0, len(a.addrs))
	for _, addr := range a.addrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

type membersMap struct {
	logger           ilogger.Logger
	failoverService  *FailoverService
	members          map[types.UUID]*pubcluster.MemberInfo
	addrToMemberUUID map[pubcluster.Address]types.UUID
	membersMu        *sync.RWMutex
	orderedMembers   []pubcluster.MemberInfo
	version          int32
}

func newMembersMap(failoverService *FailoverService, lg ilogger.Logger) membersMap {
	mm := membersMap{
		membersMu:       &sync.RWMutex{},
		failoverService: failoverService,
		logger:          lg,
	}
	mm.reset()
	return mm
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.MemberInfo, removed []pubcluster.MemberInfo) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		m.version = version
		m.orderedMembers = members
		newUUIDs := map[types.UUID]struct{}{}
		added = []pubcluster.MemberInfo{}
		for _, member := range members {
			mc := member
			if m.addMember(&mc) {
				added = append(added, mc)
			}
			newUUIDs[mc.UUID] = struct{}{}
		}
		removed = []pubcluster.MemberInfo{}
		for _, member := range m.members {
			if _, ok := newUUIDs[member.UUID]; !ok {
				m.removeMember(member)
				removed = append(removed, *member)
			}
		}
		m.logger.Trace(func() string {
			return fmt.Sprintf("cluster.Service.Update added: %v", added)
		})
		m.logger.Trace(func() string {
			return fmt.Sprintf("cluster.Service.Update removed: %v", removed)
		})
		m.logMembers(version, members)
	}
	return
}

func (m *membersMap) Find(uuid types.UUID) *pubcluster.MemberInfo {
	m.membersMu.RLock()
	member := m.members[uuid]
	m.membersMu.RUnlock()
	return member
}

func (m *membersMap) OrderedMembers() []pubcluster.MemberInfo {
	m.membersMu.Lock()
	members := make([]pubcluster.MemberInfo, len(m.orderedMembers))
	copy(members, m.orderedMembers)
	m.membersMu.Unlock()
	return members
}

// addMember adds the given memberinfo if it doesn't already exist and returns true in that case.
// If memberinfo already exists returns false.
func (m *membersMap) addMember(member *pubcluster.MemberInfo) bool {
	// synchronized in Update
	uuid := member.UUID
	addr, err := m.failoverService.Current().AddressTranslator.TranslateMember(context.TODO(), member)
	if err != nil {
		addr = member.Address
	}
	if _, uuidFound := m.members[uuid]; uuidFound {
		return false
	}
	if existingUUID, addrFound := m.addrToMemberUUID[addr]; addrFound {
		delete(m.members, existingUUID)
	}
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.membersMap.addMember: %s, %s", member.UUID.String(), addr)
	})
	m.members[uuid] = member
	m.addrToMemberUUID[addr] = uuid
	return true
}

func (m *membersMap) removeMember(member *pubcluster.MemberInfo) {
	// synchronized in Update
	m.logger.Trace(func() string {
		return fmt.Sprintf("cluster.membersMap.removeMember: %s, %s", member.UUID.String(), member.Address.String())
	})
	delete(m.members, member.UUID)
	delete(m.addrToMemberUUID, member.Address)
}

func (m *membersMap) reset() {
	m.membersMu.Lock()
	m.members = map[types.UUID]*pubcluster.MemberInfo{}
	m.addrToMemberUUID = map[pubcluster.Address]types.UUID{}
	m.version = -1
	m.membersMu.Unlock()
}

func (m *membersMap) logMembers(version int32, members []pubcluster.MemberInfo) {
	// synchronized in Update
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("\n\nMembers {size:%d, ver:%d} [\n", len(m.members), version))
	for _, mem := range members {
		sb.WriteString(fmt.Sprintf("\tMember %s - %s\n", mem.Address, mem.UUID))
	}
	sb.WriteString("]\n\n")
	m.logger.Infof(sb.String())
}
