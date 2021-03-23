package cluster

import (
	"reflect"
	"sync"
	"sync/atomic"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	publifecycle "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/lifecycle"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/codec"
)

type Service interface {
	//GetMemberByUUID(uuid string) pubcluster.Member
	//Members() []pubcluster.Member
	OwnerConnectionAddr() pubcluster.Address
}

type ServiceImpl struct {
	ownerConnectionAddr atomic.Value
	addrProviders       []AddressProvider
	ownerUUID           atomic.Value
	uuid                atomic.Value
	requestCh           chan<- invocation.Invocation
	startCh             chan struct{}
	startChMu           *sync.Mutex
	invocationFactory   invocation.Factory
	eventDispatcher     event.DispatchService
	logger              logger.Logger

	membersMap membersMap
}

type CreationBundle struct {
	AddrProviders     []AddressProvider
	RequestCh         chan<- invocation.Invocation
	InvocationFactory invocation.Factory
	EventDispatcher   event.DispatchService
	Logger            logger.Logger
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
}

func NewServiceImpl(bundle CreationBundle) *ServiceImpl {
	bundle.Check()
	service := &ServiceImpl{
		addrProviders:     bundle.AddrProviders,
		requestCh:         bundle.RequestCh,
		startCh:           make(chan struct{}, 1),
		startChMu:         &sync.Mutex{},
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		logger:            bundle.Logger,
		membersMap:        newMembersMap(),
	}
	service.ownerConnectionAddr.Store(&pubcluster.AddressImpl{})
	return service
}

//func (s *ServiceImpl) GetMemberByUUID(uuid string) pubcluster.Member {
//	s.membersMu.RLock()
//	defer s.membersMu.RUnlock()
//	if member, ok := s.members[uuid]; ok {
//		return member
//	}
//	return nil
//}

//func (s *ServiceImpl) Members() []pubcluster.Member {
//	s.membersMu.RLock()
//	defer s.membersMu.RUnlock()
//	members := make([]pubcluster.Member, 0, len(s.members))
//	for _, member := range s.members {
//		members = append(members, member)
//	}
//	return members
//}

func (s *ServiceImpl) OwnerConnectionAddr() pubcluster.Address {
	if addr, ok := s.ownerConnectionAddr.Load().(pubcluster.Address); ok {
		return addr
	}
	return nil
}

func (s *ServiceImpl) Start() <-chan struct{} {
	s.eventDispatcher.Subscribe(lifecycle.EventStateChanged, event.DefaultSubscriptionID, s.handleLifecycleStateChanged)
	s.eventDispatcher.Subscribe(EventMembersUpdated, event.DefaultSubscriptionID, s.handleMembersUpdated)
	return s.startCh
}

func (s *ServiceImpl) Stop() {
	subscriptionID := int(reflect.ValueOf(s.handleLifecycleStateChanged).Pointer())
	s.eventDispatcher.Unsubscribe(lifecycle.EventStateChanged, subscriptionID)
}

func (s *ServiceImpl) memberCandidateAddrs() []pubcluster.Address {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

func (s *ServiceImpl) handleLifecycleStateChanged(event event.Event) {
	if stateChangeEvent, ok := event.(publifecycle.StateChanged); ok {
		if stateChangeEvent.State() == publifecycle.StateClientConnected {
			go s.sendMemberListViewRequest()
		}
	}
}

func (s *ServiceImpl) handleMembersUpdated(event event.Event) {
	if membersUpdateEvent, ok := event.(MembersUpdated); ok {
		added, removed := s.membersMap.Update(membersUpdateEvent.Members(), membersUpdateEvent.Version())
		// XXX:
		s.startChMu.Lock()
		if s.startCh != nil {
			close(s.startCh)
			s.startCh = nil
		}
		s.startChMu.Unlock()
		for _, member := range added {
			s.eventDispatcher.Publish(NewMemberAdded(member))
		}
		for _, member := range removed {
			s.eventDispatcher.Publish(NewMemberRemoved(member))
		}
	}
}

func (s *ServiceImpl) sendMemberListViewRequest() {
	request := codec.EncodeClientAddClusterViewListenerRequest()
	inv := s.invocationFactory.NewInvocationOnRandomTarget(request, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.eventDispatcher.Publish(NewMembersUpdated(memberInfos, version))
		}, func(version int32, partitions []proto.Pair) {
			s.eventDispatcher.Publish(NewPartitionsUpdated(partitions, version))
		})
	})
	s.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		s.logger.Error(err)
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
	members   map[string]*Member
	membersMu *sync.RWMutex
	version   int32
}

func newMembersMap() membersMap {
	return membersMap{
		members:   map[string]*Member{},
		membersMu: &sync.RWMutex{},
		version:   -1,
	}
}

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []*Member, removed []*Member) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		membersMap := map[string]pubcluster.Member{}
		added = []*Member{}
		for _, member := range members {
			uuid := member.UUID().String()
			membersMap[uuid] = member
			if _, ok := m.members[uuid]; !ok {
				newMember := NewMember(member.Address(), member.UUID(), member.LiteMember(), member.Attributes(), member.Version(), nil)
				m.members[uuid] = newMember
				added = append(added, newMember)
			}
		}
		removed = []*Member{}
		for _, member := range m.members {
			uuid := member.UUID().String()
			if _, ok := membersMap[uuid]; !ok {
				removed = append(removed, member)
			}
		}
	}
	return
}
