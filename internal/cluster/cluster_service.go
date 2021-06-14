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
	"fmt"
	"sync"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Service struct {
	logger            ilogger.Logger
	requestCh         chan<- invocation.Invocation
	doneCh            chan struct{}
	invocationFactory *ConnectionInvocationFactory
	eventDispatcher   *event.DispatchService
	partitionService  *PartitionService
	config            *pubcluster.Config
	membersMap        membersMap
	addrProviders     []AddressProvider
}

type CreationBundle struct {
	Logger            ilogger.Logger
	Config            *pubcluster.Config
	RequestCh         chan<- invocation.Invocation
	InvocationFactory *ConnectionInvocationFactory
	EventDispatcher   *event.DispatchService
	PartitionService  *PartitionService
	AddrProviders     []AddressProvider
}

func (b CreationBundle) Check() {
	if b.AddrProviders == nil {
		panic("AddrProviders is nil")
	}
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
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

func NewServiceImpl(bundle CreationBundle) *Service {
	bundle.Check()
	return &Service{
		addrProviders:     bundle.AddrProviders,
		requestCh:         bundle.RequestCh,
		doneCh:            make(chan struct{}),
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		partitionService:  bundle.PartitionService,
		logger:            bundle.Logger,
		membersMap:        newMembersMap(),
		config:            bundle.Config,
	}
}

func (s *Service) GetMemberByUUID(uuid types.UUID) pubcluster.Member {
	return s.membersMap.Find(uuid)
}

func (s *Service) Start() {
	subscriptionID := event.MakeSubscriptionID(s.handleConnectionOpened)
	s.eventDispatcher.Subscribe(EventConnectionOpened, subscriptionID, s.handleConnectionOpened)
	if s.logger.CanLogDebug() {
		go s.logStatus()
	}
}

func (s *Service) Stop() {
	subscriptionID := event.MakeSubscriptionID(s.handleConnectionOpened)
	s.eventDispatcher.Unsubscribe(EventConnectionOpened, subscriptionID)
	close(s.doneCh)
}

func (s *Service) MemberAddrs() []string {
	return s.membersMap.MemberAddrs()
}

func (s *Service) RandomDataMember() *Member {
	return s.membersMap.RandomDataMember()
}

func (s *Service) memberCandidateAddrs() []*pubcluster.AddressImpl {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

func (s *Service) handleConnectionOpened(event event.Event) {
	if e, ok := event.(*ConnectionOpened); ok {
		go s.sendMemberListViewRequest(e.Conn)
	}
}

func (s *Service) handleMembersUpdated(conn *Connection, version int32, memberInfos []pubcluster.MemberInfo) {
	s.logger.Debug(func() string { return fmt.Sprintf("%d: members updated", conn.connectionID) })
	added, removed := s.membersMap.Update(memberInfos, version)
	if len(added) > 0 {
		s.eventDispatcher.Publish(NewMembersAdded(added))
	}
	if len(removed) > 0 {
		s.eventDispatcher.Publish(NewMemberRemoved(removed))
	}
}

func (s *Service) sendMemberListViewRequest(conn *Connection) {
	request := codec.EncodeClientAddClusterViewListenerRequest()
	inv := s.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.handleMembersUpdated(conn, version, memberInfos)
		}, func(version int32, partitions []proto.Pair) {
			s.partitionService.Update(conn.connectionID, partitions, version)
		})
	})
	s.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		s.logger.Error(err)
	}
}

func (s *Service) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.membersMap.Info(func(members map[types.UUID]*Member) {
				s.logger.Trace(func() string {
					mems := map[types.UUID]string{}
					for uuid, member := range members {
						mems[uuid] = member.Address().String()
					}
					return fmt.Sprintf("members: %#v", mems)
				})
			})
		}
	}
}

type AddrSet struct {
	addrs map[string]*pubcluster.AddressImpl
}

func NewAddrSet() AddrSet {
	return AddrSet{addrs: map[string]*pubcluster.AddressImpl{}}
}

func (a AddrSet) AddAddr(addr *pubcluster.AddressImpl) {
	a.addrs[addr.String()] = addr
}

func (a AddrSet) AddAddrs(addrs []*pubcluster.AddressImpl) {
	for _, addr := range addrs {
		a.AddAddr(addr)
	}
}

func (a AddrSet) Addrs() []*pubcluster.AddressImpl {
	addrs := make([]*pubcluster.AddressImpl, 0, len(a.addrs))
	for _, addr := range a.addrs {
		addrs = append(addrs, addr)
	}
	return addrs
}

type membersMap struct {
	members          map[types.UUID]*Member
	addrToMemberUUID map[string]types.UUID
	membersMu        *sync.RWMutex
	version          int32
}

func newMembersMap() membersMap {
	return membersMap{
		members:          map[types.UUID]*Member{},
		addrToMemberUUID: map[string]types.UUID{},
		membersMu:        &sync.RWMutex{},
		version:          -1,
	}
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.Member, removed []pubcluster.Member) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		newUUIDs := map[types.UUID]struct{}{}
		added = []pubcluster.Member{}
		for _, member := range members {
			if member := m.addMember(member); member != nil {
				added = append(added, member)
			}
			newUUIDs[member.UUID()] = struct{}{}
		}
		removed = []pubcluster.Member{}
		for _, member := range m.members {
			if _, ok := newUUIDs[member.UUID()]; !ok {
				m.removeMember(member)
				removed = append(removed, member)
			}
		}
	}
	return
}

func (m *membersMap) Find(uuid types.UUID) *Member {
	m.membersMu.RLock()
	member := m.members[uuid]
	m.membersMu.RUnlock()
	return member
}

func (m *membersMap) RemoveMembersWithAddr(addr string) {
	m.membersMu.Lock()
	if uuid, ok := m.addrToMemberUUID[addr]; ok {
		m.removeMember(m.members[uuid])
	}
	m.membersMu.Unlock()
}

func (m *membersMap) Info(infoFun func(members map[types.UUID]*Member)) {
	m.membersMu.RLock()
	infoFun(m.members)
	m.membersMu.RUnlock()
}

func (m *membersMap) MemberAddrs() []string {
	m.membersMu.RLock()
	addrs := make([]string, 0, len(m.addrToMemberUUID))
	for addr := range m.addrToMemberUUID {
		addrs = append(addrs, addr)
	}
	m.membersMu.RUnlock()
	return addrs
}

func (m *membersMap) RandomDataMember() *Member {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	for _, mem := range m.members {
		if !mem.isLiteMember {
			return mem
		}
	}
	return nil
}

func (m *membersMap) addMember(memberInfo pubcluster.MemberInfo) *Member {
	uuid := memberInfo.UUID()
	addr := memberInfo.Address().String()
	if _, uuidFound := m.members[uuid]; uuidFound {
		return nil
	}
	if existingUUID, addrFound := m.addrToMemberUUID[addr]; addrFound {
		delete(m.members, existingUUID)
	}
	member := NewMember(memberInfo.Address(), memberInfo.UUID(), memberInfo.LiteMember(), memberInfo.Attributes(), memberInfo.Version(), nil)
	m.members[uuid] = member
	m.addrToMemberUUID[addr] = uuid
	return member
}

func (m *membersMap) removeMember(member *Member) {
	delete(m.members, member.UUID())
	delete(m.addrToMemberUUID, member.Address().String())
}
