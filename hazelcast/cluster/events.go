package cluster

import "github.com/hazelcast/hazelcast-go-client/v4/internal"

type MemberState int

const (
	MemberStateAdded MemberState = iota
	MemberStateRemoved
)

type MemberStateChangedHandler func(event MemberStateChanged)

type MemberStateChanged struct {
	State  MemberState
	Member Member
}

func (e *MemberStateChanged) EventName() string {
	return internal.EventMemberStateChanged
}
