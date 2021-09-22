package sql

import "github.com/hazelcast/hazelcast-go-client/types"

type Error struct {
	Code                int32
	OriginatingMemberId types.UUID
	Message             string
}

func (e Error) Error() string {
	panic("implement me")
}
