package cluster

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/event"
)

const (
	EventMemberAdded   = "internal.cluster.memberadded"
	EventMemberRemoved = "internal.cluster.memberremoved"
)

type MemberAdded interface {
	event.Event
	Members() Member
}

type MemberRemoved interface {
	event.Event
	Members() Member
}
