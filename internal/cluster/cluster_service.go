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

const (
	smartRoutingEnabled int32 = 1
)

type Service interface {
	OwnerConnectionAddr() pubcluster.Address
	GetMemberByUUID(uuid string) pubcluster.Member
	SmartRoutingEnabled() bool
}

type ServiceImpl struct {
	ownerConnectionAddr atomic.Value
	addrProviders       []AddressProvider
	ownerUUID           atomic.Value
	uuid                atomic.Value
	requestCh           chan<- invocation.Invocation
	startCh             chan struct{}
	startChAtom         int32
	invocationFactory   invocation.Factory
	eventDispatcher     event.DispatchService
	logger              logger.Logger

	membersMap   membersMap
	smartRouting int32
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
		invocationFactory: bundle.InvocationFactory,
		eventDispatcher:   bundle.EventDispatcher,
		logger:            bundle.Logger,
		membersMap:        newMembersMap(),
	}
	service.ownerConnectionAddr.Store(&pubcluster.AddressImpl{})
	return service
}

func (s *ServiceImpl) OwnerConnectionAddr() pubcluster.Address {
	if addr, ok := s.ownerConnectionAddr.Load().(pubcluster.Address); ok {
		return addr
	}
	return nil
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) pubcluster.Member {
	return s.membersMap.Find(uuid)
}

func (s *ServiceImpl) Start(wantSmartRouting bool) <-chan struct{} {
	s.eventDispatcher.Subscribe(lifecycle.EventStateChanged, event.DefaultSubscriptionID, s.handleLifecycleStateChanged)
	s.eventDispatcher.Subscribe(EventMembersUpdated, event.DefaultSubscriptionID, s.handleMembersUpdated)
	if wantSmartRouting {
		s.listenPartitionsLoaded()
	}
	return s.startCh
}

func (s *ServiceImpl) Stop() {
	subscriptionID := int(reflect.ValueOf(s.handleLifecycleStateChanged).Pointer())
	s.eventDispatcher.Unsubscribe(lifecycle.EventStateChanged, subscriptionID)
}

func (s *ServiceImpl) SmartRoutingEnabled() bool {
	return s.smartRouting == smartRoutingEnabled
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
		if atomic.CompareAndSwapInt32(&s.startChAtom, 0, 1) {
			close(s.startCh)
			s.startCh = nil
		}
		if len(added) > 0 {
			s.eventDispatcher.Publish(NewMembersAdded(added))
		}
		if len(removed) > 0 {
			s.eventDispatcher.Publish(NewMemberRemoved(removed))
		}
	}
}

func (s *ServiceImpl) sendMemberListViewRequest() {
	request := codec.EncodeClientAddClusterViewListenerRequest()
	inv := s.invocationFactory.NewInvocationOnRandomTarget(request, func(response *proto.ClientMessage) {
		codec.HandleClientAddClusterViewListener(response, func(version int32, memberInfos []pubcluster.MemberInfo) {
			s.logger.Infof("members updated")
			s.eventDispatcher.Publish(NewMembersUpdated(memberInfos, version))
		}, func(version int32, partitions []proto.Pair) {
			s.logger.Infof("partitions updated")
			s.eventDispatcher.Publish(NewPartitionsUpdated(partitions, version))
		})
	})
	s.requestCh <- inv
	if _, err := inv.Get(); err != nil {
		s.logger.Error(err)
	}
}

func (s *ServiceImpl) listenPartitionsLoaded() {
	subscriptionID := event.MakeSubscriptionID(s.enableSmartRouting)
	handler := func(event event.Event) {
		s.logger.Info("enabling smart routing")
		s.enableSmartRouting()
		s.eventDispatcher.Unsubscribe(EventPartitionsLoaded, subscriptionID)
	}
	s.eventDispatcher.Subscribe(EventPartitionsLoaded, subscriptionID, handler)
}

func (s *ServiceImpl) enableSmartRouting() {
	atomic.StoreInt32(&s.smartRouting, smartRoutingEnabled)
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

func (m *membersMap) Update(members []pubcluster.MemberInfo, version int32) (added []pubcluster.Member, removed []pubcluster.Member) {
	m.membersMu.Lock()
	defer m.membersMu.Unlock()
	if version > m.version {
		membersMap := map[string]pubcluster.Member{}
		added = []pubcluster.Member{}
		for _, member := range members {
			uuid := member.UUID().String()
			membersMap[uuid] = member
			if _, ok := m.members[uuid]; !ok {
				newMember := NewMember(member.Address(), member.UUID(), member.LiteMember(), member.Attributes(), member.Version(), nil)
				m.members[uuid] = newMember
				added = append(added, newMember)
			}
		}
		removed = []pubcluster.Member{}
		for _, member := range m.members {
			uuid := member.UUID().String()
			if _, ok := membersMap[uuid]; !ok {
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
