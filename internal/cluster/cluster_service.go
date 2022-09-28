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
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type Service struct {
	logger            logger.LogAdaptor
	config            *pubcluster.Config
	eventDispatcher   *event.DispatchService
	partitionService  *PartitionService
	failoverService   *FailoverService
	invocationService *invocation.Service
	invocationFactory *ConnectionInvocationFactory
	membersMap        membersMap
}

type CreationBundle struct {
	Logger            logger.LogAdaptor
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
	if b.Logger.Logger == nil {
		panic("LogAdaptor is nil")
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

func (s *Service) SQLMember() *pubcluster.MemberInfo {
	return s.membersMap.SQLMember()
}

func (s *Service) RefreshedSeedAddrs(clusterCtx *CandidateCluster) ([]pubcluster.Address, error) {
	s.membersMap.reset()
	return uniqueAddrs(clusterCtx.AddressProvider)
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

// uniqueAddrs return unique addresses while preserving initial order
func uniqueAddrs(ap AddressProvider) ([]pubcluster.Address, error) {
	addrs, err := ap.Addresses()
	if err != nil {
		return nil, err
	}
	l := len(addrs)
	uniqueSet := make(map[pubcluster.Address]struct{}, l)
	uniqueAddrs := make([]pubcluster.Address, 0, l)
	for _, a := range addrs {
		if _, ok := uniqueSet[a]; ok {
			continue
		}
		uniqueAddrs = append(uniqueAddrs, a)
	}
	return uniqueAddrs, nil
}

type membersMap struct {
	logger             logger.LogAdaptor
	failoverService    *FailoverService
	members            map[types.UUID]*pubcluster.MemberInfo
	addrToMemberUUID   map[pubcluster.Address]types.UUID
	membersMu          *sync.RWMutex
	orderedMembers     atomic.Value
	majorityMajorMinor uint16
	version            int32
}

func newMembersMap(failoverService *FailoverService, lg logger.LogAdaptor) membersMap {
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
		m.orderedMembers.Store(members)
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
		if len(added) > 0 || len(removed) > 0 {
			// there is a change in the cluster
			v, err := LargerGroupMajorMinorVersion(members)
			if err != nil {
				m.logger.Warnf(err.Error())
			}
			m.majorityMajorMinor = v
		}
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
	return m.orderedMembers.Load().([]pubcluster.MemberInfo)
}

func (m *membersMap) SQLMember() *pubcluster.MemberInfo {
	m.membersMu.RLock()
	defer m.membersMu.RUnlock()
	if m.majorityMajorMinor == 0 {
		return nil
	}
	for _, mem := range m.members {
		if mem.LiteMember {
			continue
		}
		if mem.Version.MajorMinor() == m.majorityMajorMinor {
			// since maps in Go is randomized, this will return a random member
			return mem
		}
	}
	return nil
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
	m.logger.Info(func() string {
		sb := strings.Builder{}
		sb.WriteString(fmt.Sprintf("\n\nMembers {size:%d, ver:%d} [\n", len(m.members), version))
		for _, mem := range members {
			sb.WriteString(fmt.Sprintf("\tMember %s - %s\n", mem.Address, mem.UUID))
		}
		sb.WriteString("]\n\n")
		return sb.String()
	})
}

// LargerGroupMajorMinorVersion finds the version of the most numerous member group by version.
func LargerGroupMajorMinorVersion(members []pubcluster.MemberInfo) (uint16, error) {
	// synchronized in Update
	// The members should have at most 2 different version (ignoring the patch version).
	var vs [2]uint16
	var vv [2]pubcluster.MemberVersion
	var count [2]int
	var next int
	for _, mem := range members {
		if mem.LiteMember {
			continue
		}
		v := mem.Version.MajorMinor()
		if v == vs[0] {
			count[0]++
		} else if v == vs[1] {
			count[1]++
		} else if next < 2 {
			vs[next] = v
			vv[next] = mem.Version
			count[next]++
			next++
		} else {
			return 0, fmt.Errorf("more than 2 distinct member versions found: %s, %s, %s", vv[0], vv[1], mem.Version)
		}
	}
	if count[0] == 0 {
		// there are no data members
		return 0, nil
	}
	if count[0] > count[1] {
		return vs[0], nil
	} else if count[0] < count[1] {
		return vs[1], nil
	} else {
		// if the counts are equal, return the newer one
		if vs[0] > vs[1] {
			return vs[0], nil
		}
		return vs[1], nil
	}
}
