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
	"sync"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
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
	addrTranslator    AddressTranslator
}

type CreationBundle struct {
	Logger            ilogger.Logger
	Config            *pubcluster.Config
	RequestCh         chan<- invocation.Invocation
	InvocationFactory *ConnectionInvocationFactory
	EventDispatcher   *event.DispatchService
	PartitionService  *PartitionService
	AddrProviders     []AddressProvider
	AddressTranslator AddressTranslator
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
	if b.AddressTranslator == nil {
		panic("AddressTranslator is nil")
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
		membersMap:        newMembersMap(bundle.AddressTranslator),
		config:            bundle.Config,
		addrTranslator:    bundle.AddressTranslator,
	}
}

func (s *Service) GetMemberByUUID(uuid string) *pubcluster.MemberInfo {
	return s.membersMap.Find(uuid)
}

func (s *Service) Start() {
	if s.logger.CanLogDebug() {
		go s.logStatus()
	}
}

func (s *Service) Stop() {
	close(s.doneCh)
}

func (s *Service) MemberAddrs() []pubcluster.Address {
	return s.membersMap.MemberAddrs()
}

func (s *Service) SeedAddrs() []pubcluster.Address {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

func (s *Service) MemberAddr(m *pubcluster.MemberInfo) (pubcluster.Address, error) {
	return s.addrTranslator.TranslateMember(context.TODO(), m)
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

func (s *Service) sendMemberListViewRequest(ctx context.Context, conn *Connection) error {
	request := codec.EncodeClientAddClusterViewListenerRequest()
	inv := s.invocationFactory.NewConnectionBoundInvocation(request, -1, nil, conn, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.handleMembersUpdated(conn, version, memberInfos)
		}, func(version int32, partitions []proto.Pair) {
			s.partitionService.Update(conn.connectionID, partitions, version)
		})
	})
	s.requestCh <- inv
	_, err := inv.GetWithContext(ctx)
	return err
}

func (s *Service) logStatus() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-s.doneCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.membersMap.Info(func(members map[string]*pubcluster.MemberInfo) {
				s.logger.Trace(func() string {
					mems := map[string]pubcluster.Address{}
					for uuid, member := range members {
						mems[uuid] = member.Address
					}
					return fmt.Sprintf("members: %#v", mems)
				})
			})
		}
	}
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
	members          map[string]*pubcluster.MemberInfo
	addrToMemberUUID map[pubcluster.Address]string
	membersMu        *sync.RWMutex
	version          int32
	addrTranslator   AddressTranslator
}

func newMembersMap(translator AddressTranslator) membersMap {
	return membersMap{
		members:          map[string]*pubcluster.MemberInfo{},
		addrToMemberUUID: map[pubcluster.Address]string{},
		membersMu:        &sync.RWMutex{},
		version:          -1,
		addrTranslator:   translator,
	}
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.MemberInfo, removed []pubcluster.MemberInfo) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		newUUIDs := map[string]struct{}{}
		added = []pubcluster.MemberInfo{}
		for _, member := range members {
			if m.addMember(&member) {
				added = append(added, member)
			}
			newUUIDs[member.UUID.String()] = struct{}{}
		}
		removed = []pubcluster.MemberInfo{}
		for _, member := range m.members {
			if _, ok := newUUIDs[member.UUID.String()]; !ok {
				m.removeMember(member)
				removed = append(removed, *member)
			}
		}
	}
	return
}

func (m *membersMap) Find(uuid string) *pubcluster.MemberInfo {
	m.membersMu.RLock()
	member := m.members[uuid]
	m.membersMu.RUnlock()
	return member
}

func (m *membersMap) RemoveMembersWithAddr(addr pubcluster.Address) {
	m.membersMu.Lock()
	if uuid, ok := m.addrToMemberUUID[addr]; ok {
		m.removeMember(m.members[uuid])
	}
	m.membersMu.Unlock()
}

func (m *membersMap) Info(infoFun func(members map[string]*pubcluster.MemberInfo)) {
	m.membersMu.RLock()
	infoFun(m.members)
	m.membersMu.RUnlock()
}

func (m *membersMap) MemberAddrs() []pubcluster.Address {
	m.membersMu.RLock()
	addrs := make([]pubcluster.Address, 0, len(m.addrToMemberUUID))
	for addr := range m.addrToMemberUUID {
		addrs = append(addrs, addr)
	}
	m.membersMu.RUnlock()
	return addrs
}

// addMember adds the given memberinfo if it doesn't already exist and returns true in that case.
// If memberinfo already exists returns false.
func (m *membersMap) addMember(memberInfo *pubcluster.MemberInfo) bool {
	uuid := memberInfo.UUID.String()
	addr, err := m.addrTranslator.TranslateMember(context.TODO(), memberInfo)
	if err != nil {
		addr = memberInfo.Address
	}
	if _, uuidFound := m.members[uuid]; uuidFound {
		return false
	}
	if existingUUID, addrFound := m.addrToMemberUUID[addr]; addrFound {
		delete(m.members, existingUUID)
	}
	m.members[uuid] = memberInfo
	m.addrToMemberUUID[addr] = uuid
	return true
}

func (m *membersMap) removeMember(member *pubcluster.MemberInfo) {
	delete(m.members, member.UUID.String())
	delete(m.addrToMemberUUID, member.Address)
}
