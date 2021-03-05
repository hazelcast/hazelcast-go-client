package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"sync/atomic"
)

type Service interface {
	GetMemberByUUID(uuid string) core.Member
	Members() []core.Member
	OwnerConnectionAddr() *core.Address
}

type ServiceImpl struct {
	ownerConnectionAddr atomic.Value
	addrProviders       []AddressProvider
}

func NewServiceImpl(addrProviders []AddressProvider) *ServiceImpl {
	service := &ServiceImpl{addrProviders: addrProviders}
	service.ownerConnectionAddr.Store(&core.Address{})
	return service
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) core.Member {
	panic("implement me")
}

func (s *ServiceImpl) Members() []core.Member {
	panic("implement me")
}

func (s *ServiceImpl) OwnerConnectionAddr() *core.Address {
	if addr, ok := s.ownerConnectionAddr.Load().(*core.Address); ok {
		return addr
	}
	return nil
}

func (s *ServiceImpl) memberCandidateAddrs() []*core.Address {
	addrSet := AddrSet{}
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
}

type AddrSet struct {
	addrs map[string]*core.Address
}

func (a AddrSet) AddAddr(addr *core.Address) {
	a.addrs[addr.String()] = addr
}

func (a AddrSet) AddAddrs(addrs []*core.Address) {
	for _, addr := range addrs {
		a.AddAddr(addr)
	}
}

func (a AddrSet) Addrs() []*core.Address {
	addrs := make([]*core.Address, 0, len(a.addrs))
	for _, addr := range a.addrs {
		addrs = append(addrs, addr)
	}
	return addrs
}
