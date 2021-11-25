package sql

import "github.com/hazelcast/hazelcast-go-client/types"

type Error struct {
	Message             string
	OriginatingMemberId types.UUID
	Code                int32
}

func (e Error) Error() string {
	return e.Message
}
