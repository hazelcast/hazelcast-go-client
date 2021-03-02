package cluster

import "github.com/hazelcast/hazelcast-go-client/v4/internal/core"

type Service interface {
	GetMemberByUUID(uuid string) core.Member
	OwnerConnectionAddress() *core.Address
}
