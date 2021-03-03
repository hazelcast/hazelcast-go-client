package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"sync/atomic"
)

type Service interface {
	GetMemberByUUID(uuid string) core.Member
	Members() []core.Member
	OwnerConnectionAddress() *core.Address
}

type ServiceImpl struct {
	ownerConnectionAddress atomic.Value
	addressProviders       []AddressProvider
}

func NewServiceImpl(addressProviders []AddressProvider) *ServiceImpl {
	service := &ServiceImpl{addressProviders: addressProviders}
	service.ownerConnectionAddress.Store(&core.Address{})
	return service
}

func (s *ServiceImpl) GetMemberByUUID(uuid string) core.Member {
	panic("implement me")
}

func (s *ServiceImpl) Members() []core.Member {
	panic("implement me")
}

func (s *ServiceImpl) OwnerConnectionAddress() *core.Address {
	if addr, ok := s.ownerConnectionAddress.Load().(*core.Address); ok {
		return addr
	}
	return nil
}
