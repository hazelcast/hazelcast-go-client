package cluster

import (
	"sync/atomic"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
)

type Service interface {
	GetMemberByUUID(uuid string) pubcluster.Member
	Members() []pubcluster.Member
	OwnerConnectionAddr() pubcluster.Address
}

type ServiceImpl struct {
	ownerConnectionAddr atomic.Value
	addrProviders       []AddressProvider
	ownerUUID           atomic.Value
	uuid                atomic.Value
}

func NewServiceImpl(addrProviders []AddressProvider) *ServiceImpl {
	service := &ServiceImpl{addrProviders: addrProviders}
	service.ownerConnectionAddr.Store(&pubcluster.AddressImpl{})
	return service
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) pubcluster.Member {
	panic("implement me")
}

func (s *ServiceImpl) Members() []pubcluster.Member {
	panic("implement me")
}

func (s *ServiceImpl) OwnerConnectionAddr() pubcluster.Address {
	if addr, ok := s.ownerConnectionAddr.Load().(pubcluster.Address); ok {
		return addr
	}
	return nil
}

func (s *ServiceImpl) memberCandidateAddrs() []pubcluster.Address {
	addrSet := NewAddrSet()
	for _, addrProvider := range s.addrProviders {
		addrSet.AddAddrs(addrProvider.Addresses())
	}
	return addrSet.Addrs()
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
