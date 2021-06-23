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
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Service struct {
	logger            ilogger.Logger
	addrTranslator    AddressTranslator
	addrProvider      AddressProvider
	config            *pubcluster.Config
	eventDispatcher   *event.DispatchService
	partitionService  *PartitionService
	requestCh         chan<- invocation.Invocation
	invocationFactory *ConnectionInvocationFactory
	doneCh            chan struct{}
	membersMap        membersMap
}

type CreationBundle struct {
	Logger            ilogger.Logger
	AddressTranslator AddressTranslator
	RequestCh         chan<- invocation.Invocation
	InvocationFactory *ConnectionInvocationFactory
	EventDispatcher   *event.DispatchService
	PartitionService  *PartitionService
	Config            *pubcluster.Config
	AddrProvider      AddressProvider
}

func (b CreationBundle) Check() {
	if b.AddrProvider == nil {
		panic("AddrProvider is nil")
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

func NewService(bundle CreationBundle) *Service {
	bundle.Check()
	return &Service{
		addrProvider:      bundle.AddrProvider,
		requestCh:         bundle.RequestCh,
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		partitionService:  bundle.PartitionService,
		logger:            bundle.Logger,
		config:            bundle.Config,
		addrTranslator:    bundle.AddressTranslator,
	}
}

func (s *Service) GetMemberByUUID(uuid types.UUID) *pubcluster.MemberInfo {
	return s.membersMap.Find(uuid)
}

func (s *Service) Start() {
	s.reset()
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

func (s *Service) RandomDataMember() *pubcluster.MemberInfo {
	return s.membersMap.RandomDataMember()
}

func (s *Service) RandomDataMemberExcluding(excluded map[pubcluster.Address]struct{}) *pubcluster.MemberInfo {
	return s.membersMap.RandomDataMemberExcluding(excluded)
}

func (s *Service) RefreshedSeedAddrs(refresh bool) []pubcluster.Address {
	s.membersMap.reset()
	addrSet := NewAddrSet()
	addrSet.AddAddrs(s.addrProvider.Addresses(refresh))
	return addrSet.Addrs()
}

func (s *Service) MemberAddr(m *pubcluster.MemberInfo) (pubcluster.Address, error) {
	return s.addrTranslator.TranslateMember(context.TODO(), m)
}

func (s *Service) reset() {
	s.doneCh = make(chan struct{}, 1)
	s.membersMap = newMembersMap(s.addrTranslator, s.logger)
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
	inv := s.invocationFactory.NewConnectionBoundInvocation(request, conn, func(response *proto.ClientMessage) {
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
			s.membersMap.Info(func(members map[types.UUID]*pubcluster.MemberInfo) {
				s.logger.Trace(func() string {
					mems := map[types.UUID]pubcluster.Address{}
					for uuid, member := range members {
						mems[uuid] = member.Address
					}
					return fmt.Sprintf("members: %+v", mems)
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
	addrTranslator   AddressTranslator
	members          map[types.UUID]*pubcluster.MemberInfo
	addrToMemberUUID map[pubcluster.Address]types.UUID
	membersMu        *sync.RWMutex
	version          int32
	logger           ilogger.Logger
}

func newMembersMap(translator AddressTranslator, lg ilogger.Logger) membersMap {
	mm := membersMap{
		membersMu:      &sync.RWMutex{},
		addrTranslator: translator,
		logger:         lg,
	}
	mm.reset()
	return mm
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.MemberInfo, removed []pubcluster.MemberInfo) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		newUUIDs := map[types.UUID]struct{}{}
		added = []pubcluster.MemberInfo{}
		for _, member := range members {
			if m.addMember(&member) {
				added = append(added, member)
			}
			newUUIDs[member.UUID] = struct{}{}
		}
		removed = []pubcluster.MemberInfo{}
		for _, member := range m.members {
			if _, ok := newUUIDs[member.UUID]; !ok {
				m.removeMember(member)
				removed = append(removed, *member)
			}
		}
	}
	return
}

func (m *membersMap) Reset() {
	m.membersMu.Lock()
	m.reset()
	m.membersMu.Unlock()
}

func (m *membersMap) Find(uuid types.UUID) *pubcluster.MemberInfo {
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

func (m *membersMap) Info(infoFun func(members map[types.UUID]*pubcluster.MemberInfo)) {
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

// RandomDataMember returns a data member.
// Returns nil if no suitable data member is found.
func (m *membersMap) RandomDataMember() *pubcluster.MemberInfo {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	for _, mem := range m.members {
		if !mem.LiteMember {
			return mem
		}
	}
	return nil
}

// RandomDataMemberExcluding returns a data member not excluded in the given map.
// Returns nil if no suitable data member is found.
// Panics if excluded map is nil.
func (m *membersMap) RandomDataMemberExcluding(excluded map[pubcluster.Address]struct{}) *pubcluster.MemberInfo {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	for _, mem := range m.members {
		if !mem.LiteMember {
			if _, found := excluded[mem.Address]; !found {
				return mem
			}
		}
	}
	return nil
}

// addMember adds the given memberinfo if it doesn't already exist and returns true in that case.
// If memberinfo already exists returns false.
func (m *membersMap) addMember(member *pubcluster.MemberInfo) bool {
	uuid := member.UUID
	addr, err := m.addrTranslator.TranslateMember(context.TODO(), member)
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
		return fmt.Sprintf("membersMap.addMember: %s, %s", member.UUID.String(), addr)
	})
	m.members[uuid] = member
	m.addrToMemberUUID[addr] = uuid
	return true
}

func (m *membersMap) removeMember(member *pubcluster.MemberInfo) {
	m.logger.Trace(func() string {
		return fmt.Sprintf("membersMap.removeMember: %s, %s", member.UUID.String(), member.Address.String())
	})
	delete(m.members, member.UUID)
	delete(m.addrToMemberUUID, member.Address)
}

func (m *membersMap) reset() {
	m.members = map[types.UUID]*pubcluster.MemberInfo{}
	m.addrToMemberUUID = map[pubcluster.Address]types.UUID{}
	m.version = -1
}
