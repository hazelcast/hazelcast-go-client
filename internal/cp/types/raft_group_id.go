package types

import "github.com/hazelcast/hazelcast-go-client/types"

type RaftGroupId struct {
	*types.CPGroupId
	Seed int64
}
