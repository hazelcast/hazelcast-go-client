package cluster

import "github.com/hazelcast/hazelcast-go-client/v4/internal/core"

type ServiceImpl struct {
}

func NewServiceImpl() *ServiceImpl {
	return &ServiceImpl{}
}

func (i ServiceImpl) GetMemberByUUID(uuid string) core.Member {
	panic("implement me")
}

func (i ServiceImpl) OwnerConnectionAddress() *core.Address {
	panic("implement me")
}
