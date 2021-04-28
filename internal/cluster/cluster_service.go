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
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

const (
	smartRoutingEnabled int32 = 1
)

type ServiceImpl struct {
	addrProviders     []AddressProvider
	requestCh         chan<- invocation.Invocation
	doneCh            chan struct{}
	startCh           chan struct{}
	startChAtom       int32
	invocationFactory *ConnectionInvocationFactory
	eventDispatcher   *event.DispatchService
	logger            ilogger.Logger
	config            *pubcluster.Config

	membersMap membersMap
}

type CreationBundle struct {
	AddrProviders     []AddressProvider
	RequestCh         chan<- invocation.Invocation
	InvocationFactory *ConnectionInvocationFactory
	EventDispatcher   *event.DispatchService
	Logger            ilogger.Logger
	Config            *pubcluster.Config
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
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.Config == nil {
		panic("Config is nil")
	}
}

func NewServiceImpl(bundle CreationBundle) *ServiceImpl {
	bundle.Check()
	return &ServiceImpl{
		addrProviders:     bundle.AddrProviders,
		requestCh:         bundle.RequestCh,
		doneCh:            make(chan struct{}),
		startCh:           make(chan struct{}),
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		logger:            bundle.Logger,
		membersMap:        newMembersMap(),
		config:            bundle.Config,
	}
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) pubcluster.Member {
	return s.membersMap.Find(uuid)
}

func (s *ServiceImpl) Start() {
	subscriptionID := event.MakeSubscriptionID(s.handleConnectionOpened)
	s.eventDispatcher.Subscribe(EventConnectionOpened, subscriptionID, s.handleConnectionOpened)
	s.eventDispatcher.Subscribe(EventMembersUpdated, event.DefaultSubscriptionID, s.handleMembersUpdated)
	go s.logStatus()
}

func (s *ServiceImpl) Stop() {
	subscriptionID := event.MakeSubscriptionID(s.handleConnectionOpened)
	s.eventDispatcher.Unsubscribe(EventConnectionOpened, subscriptionID)
	close(s.doneCh)
}

func (s *ServiceImpl) MemberAddrs() []string {
	return s.membersMap.MemberAddrs()
}

func (s *ServiceImpl) memberCandidateAddrs() []*pubcluster.AddressImpl {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

func (s *ServiceImpl) handleConnectionOpened(event event.Event) {
	if e, ok := event.(*ConnectionOpened); ok {
		go s.sendMemberListViewRequest(e.Conn)
	}
}

func (s *ServiceImpl) handleMembersUpdated(event event.Event) {
	if membersUpdateEvent, ok := event.(*MembersUpdated); ok {
		added, removed := s.membersMap.Update(membersUpdateEvent.Members, membersUpdateEvent.Version)
		if atomic.CompareAndSwapInt32(&s.startChAtom, 0, 1) {
			close(s.startCh)
		}
		if len(added) > 0 {
			s.eventDispatcher.Publish(NewMembersAdded(added))
		}
		if len(removed) > 0 {
			s.eventDispatcher.Publish(NewMemberRemoved(removed))
		}
	}
}

func (s *ServiceImpl) sendMemberListViewRequest(conn *Connection) {
	request := codec.EncodeClientAddClusterViewListenerRequest()
	inv := s.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.logger.Debug(func() string { return "members updated" })
			s.eventDispatcher.Publish(NewMembersUpdated(memberInfos, version))
		}, func(version int32, partitions []proto.Pair) {
			s.eventDispatcher.Publish(NewPartitionsUpdated(partitions, version, conn.connectionID))
			s.logger.Debug(func() string { return "partitions updated" })
		})
	})
	s.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		s.logger.Error(err)
	}
}

func (s *ServiceImpl) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.membersMap.Info(func(members map[string]*Member) {
				s.logger.Trace(func() string {
					mems := map[string]string{}
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
	members          map[string]*Member
	addrToMemberUUID map[string]string
	membersMu        *sync.RWMutex
	version          int32
}

func newMembersMap() membersMap {
	return membersMap{
		members:          map[string]*Member{},
		addrToMemberUUID: map[string]string{},
		membersMu:        &sync.RWMutex{},
		version:          -1,
	}
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.Member, removed []pubcluster.Member) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		newUUIDs := map[string]struct{}{}
		added = []pubcluster.Member{}
		for _, member := range members {
			if member := m.addMember(member); member != nil {
				added = append(added, member)
			}
			newUUIDs[member.Uuid().String()] = struct{}{}
		}
		removed = []pubcluster.Member{}
		for _, member := range m.members {
			if _, ok := newUUIDs[member.Uuid().String()]; !ok {
				m.removeMember(member)
				removed = append(removed, member)
			}
		}
	}
	return
}

func (m *membersMap) Find(uuid string) *Member {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	if member, ok := m.members[uuid]; ok {
		return member
	} else {
		return nil
	}
}

func (m *membersMap) RemoveMembersWithAddr(addr string) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if uuid, ok := m.addrToMemberUUID[addr]; ok {
		m.removeMember(m.members[uuid])
	}
}

func (m *membersMap) Info(infoFun func(members map[string]*Member)) {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	infoFun(m.members)
}

func (m *membersMap) MemberAddrs() []string {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	addrs := make([]string, 0, len(m.addrToMemberUUID))
	for addr := range m.addrToMemberUUID {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (m *membersMap) addMember(memberInfo pubcluster.MemberInfo) *Member {
	uuid := memberInfo.Uuid().String()
	addr := memberInfo.Address().String()
	if _, uuidFound := m.members[uuid]; uuidFound {
		return nil
	}
	if existingUUID, addrFound := m.addrToMemberUUID[addr]; addrFound {
		delete(m.members, existingUUID)
	}
	member := NewMember(memberInfo.Address(), memberInfo.Uuid(), memberInfo.LiteMember(), memberInfo.Attributes(), memberInfo.Version(), nil)
	m.members[uuid] = member
	m.addrToMemberUUID[addr] = uuid
	return member
}

func (m *membersMap) removeMember(member *Member) {
	delete(m.members, member.Uuid().String())
	delete(m.addrToMemberUUID, member.Address().String())
}
